# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Auth
    class Aws
      # Raised when trying to authorize with an invalid configuration
      #
      # @api private
      class CredentialsNotFound < Mongo::Error::AuthError
        def initialize
          super("Could not locate AWS credentials (checked Client URI and Ruby options, environment variables, ECS and EC2 metadata, and Web Identity)")
        end
      end

      # Retrieves AWS credentials from a variety of sources.
      #
      # This class provides for AWS credentials retrieval from:
      # - the passed user (which receives the credentials passed to the
      #   client via URI options and Ruby options)
      # - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
      #   environment variables (commonly used by AWS SDKs and various tools,
      #   as well as AWS Lambda)
      # - AssumeRoleWithWebIdentity API call
      # - EC2 metadata endpoint
      # - ECS metadata endpoint
      #
      # The sources listed above are consulted in the order specified.
      # The first source that contains any of the three credential components
      # (access key id, secret access key or session token) is used.
      # The credential components must form a valid set if any of the components
      # is specified; meaning, access key id and secret access key must
      # always be provided together, and if a session token is provided
      # the key id and secret key must also be provided. If a source provides
      # partial credentials, credential retrieval fails with an exception.
      #
      # @api private
      class CredentialsRetriever
        # Timeout for metadata operations, in seconds.
        #
        # The auth spec suggests a 10 second timeout but this seems
        # excessively long given that the endpoint is essentially local.
        METADATA_TIMEOUT = 5

        # @param [ Auth::User | nil ] user The user object, if one was provided.
        # @param [ Auth::Aws::CredentialsCache ] credentials_cache The credentials cache.
        def initialize(user = nil, credentials_cache: CredentialsCache.instance)
          @user = user
          @credentials_cache = credentials_cache
        end

        # @return [ Auth::User | nil ] The user object, if one was provided.
        attr_reader :user

        # Retrieves a valid set of credentials, if possible, or raises
        # Auth::InvalidConfiguration.
        #
        # @return [ Auth::Aws::Credentials ] A valid set of credentials.
        #
        # @raise Auth::InvalidConfiguration if a source contains an invalid set
        #   of credentials.
        # @raise Auth::Aws::CredentialsNotFound if credentials could not be
        #   retrieved from any source.
        def credentials
          credentials = credentials_from_user(user)
          return credentials unless credentials.nil?

          credentials = credentials_from_environment
          return credentials unless credentials.nil?

          credentials = @credentials_cache.fetch { obtain_credentials_from_endpoints }
          return credentials unless credentials.nil?

          raise Auth::Aws::CredentialsNotFound
        end

        private

        # Returns credentials from the user object.
        #
        # @param [ Auth::User | nil ] user The user object, if one was provided.
        #
        # @return [ Auth::Aws::Credentials | nil ] A set of credentials, or nil
        #
        # @raise Auth::InvalidConfiguration if a source contains an invalid set
        #   of credentials.
        def credentials_from_user(user)
          return nil unless user

          credentials = Credentials.new(
            user.name,
            user.password,
            user.auth_mech_properties['aws_session_token']
          )
          return credentials if credentials_valid?(credentials, 'Mongo::Client URI or Ruby options')
        end

        # Returns credentials from environment variables.
        #
        # @return [ Auth::Aws::Credentials | nil ] A set of credentials, or nil
        #   if retrieval failed or the obtained credentials are invalid.
        #
        # @raise Auth::InvalidConfiguration if a source contains an invalid set
        #   of credentials.
        def credentials_from_environment
          credentials = Credentials.new(
            ENV['AWS_ACCESS_KEY_ID'],
            ENV['AWS_SECRET_ACCESS_KEY'],
            ENV['AWS_SESSION_TOKEN']
          )
          credentials if credentials && credentials_valid?(credentials, 'environment variables')
        end

        # Returns credentials from the AWS metadata endpoints.
        #
        # @return [ Auth::Aws::Credentials | nil ] A set of credentials, or nil
        #   if retrieval failed or the obtained credentials are invalid.
        #
        # @raise Auth::InvalidConfiguration if a source contains an invalid set
        #   of credentials.
        def obtain_credentials_from_endpoints
          if (credentials = web_identity_credentials) && credentials_valid?(credentials, 'Web identity token')
            credentials
          elsif (credentials = ecs_metadata_credentials) && credentials_valid?(credentials, 'ECS task metadata')
            credentials
          elsif (credentials = ec2_metadata_credentials) && credentials_valid?(credentials, 'EC2 instance metadata')
            credentials
          end
        end

        # Returns credentials from the EC2 metadata endpoint. The credentials
        # could be empty, partial or invalid.
        #
        # @return [ Auth::Aws::Credentials | nil ] A set of credentials, or nil
        #   if retrieval failed.
        def ec2_metadata_credentials
          http = Net::HTTP.new('169.254.169.254')
          req = Net::HTTP::Put.new('/latest/api/token',
            # The TTL is required in order to obtain the metadata token.
            {'x-aws-ec2-metadata-token-ttl-seconds' => '30'})
          resp = ::Timeout.timeout(METADATA_TIMEOUT) do
            http.request(req)
          end
          if resp.code != '200'
            return nil
          end
          metadata_token = resp.body
          resp = ::Timeout.timeout(METADATA_TIMEOUT) do
            http_get(http, '/latest/meta-data/iam/security-credentials', metadata_token)
          end
          if resp.code != '200'
            return nil
          end
          role_name = resp.body
          escaped_role_name = CGI.escape(role_name).gsub('+', '%20')
          resp = ::Timeout.timeout(METADATA_TIMEOUT) do
            http_get(http, "/latest/meta-data/iam/security-credentials/#{escaped_role_name}", metadata_token)
          end
          if resp.code != '200'
            return nil
          end
          payload = JSON.parse(resp.body)
          unless payload['Code'] == 'Success'
            return nil
          end
          Credentials.new(
            payload['AccessKeyId'],
            payload['SecretAccessKey'],
            payload['Token'],
            DateTime.parse(payload['Expiration']).to_time
          )
        # When trying to use the EC2 metadata endpoint on ECS:
        # Errno::EINVAL: Failed to open TCP connection to 169.254.169.254:80 (Invalid argument - connect(2) for "169.254.169.254" port 80)
        rescue ::Timeout::Error, IOError, SystemCallError, TypeError
          return nil
        end

        def ecs_metadata_credentials
          relative_uri = ENV['AWS_CONTAINER_CREDENTIALS_RELATIVE_URI']
          if relative_uri.nil? || relative_uri.empty?
            return nil
          end

          http = Net::HTTP.new('169.254.170.2')
          # Per https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
          # the value in AWS_CONTAINER_CREDENTIALS_RELATIVE_URI includes
          # the leading slash.
          # The current language in MONGODB-AWS specification implies that
          # a leading slash must be added by the driver, but this is not
          # in fact needed.
          req = Net::HTTP::Get.new(relative_uri)
          resp = ::Timeout.timeout(METADATA_TIMEOUT) do
            http.request(req)
          end
          if resp.code != '200'
            return nil
          end
          payload = JSON.parse(resp.body)
          Credentials.new(
            payload['AccessKeyId'],
            payload['SecretAccessKey'],
            payload['Token'],
            DateTime.parse(payload['Expiration']).to_time
          )
        rescue ::Timeout::Error, IOError, SystemCallError, TypeError
          return nil
        end

        # Returns credentials associated with web identity token that is
        # stored in a file. This authentication mechanism is used to authenticate
        # inside EKS. See https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html
        # for further details.
        #
        # @return [ Auth::Aws::Credentials | nil ] A set of credentials, or nil
        #   if retrieval failed.
        def web_identity_credentials
          web_identity_token, role_arn, role_session_name = prepare_web_identity_inputs
          return nil if web_identity_token.nil?
          response = request_web_identity_credentials(
            web_identity_token, role_arn, role_session_name
          )
          return if response.nil?
          credentials_from_web_identity_response(response)
        end

        # Returns inputs for the AssumeRoleWithWebIdentity AWS API call.
        #
        # @return [ Array<String | nil, String | nil, String | nil> ] Web
        #   identity token, role arn, and role session name.
        def prepare_web_identity_inputs
          token_file = ENV['AWS_WEB_IDENTITY_TOKEN_FILE']
          role_arn = ENV['AWS_ROLE_ARN']
          if token_file.nil? || role_arn.nil?
            return nil
          end
          web_identity_token = File.open(token_file).read
          role_session_name = ENV['AWS_ROLE_SESSION_NAME']
          if role_session_name.nil?
            role_session_name = "ruby-app-#{SecureRandom.alphanumeric(50)}"
          end
          [web_identity_token, role_arn, role_session_name]
        rescue Errno::ENOENT, IOError, SystemCallError
          nil
        end

        # Calls AssumeRoleWithWebIdentity to obtain credentials for the
        # given web identity token.
        #
        # @param [ String ] token The OAuth 2.0 access token or
        #   OpenID Connect ID token that is provided by the identity provider.
        # @param [ String ] role_arn The Amazon Resource Name (ARN) of the role
        #   that the caller is assuming.
        # @param [ String ] role_session_name An identifier for the assumed
        #   role session.
        #
        # @return [ Net::HTTPResponse | nil ] AWS API response if successful,
        #   otherwise nil.
        def request_web_identity_credentials(token, role_arn, role_session_name)
          uri = URI('https://sts.amazonaws.com/')
          params = {
            'Action' => 'AssumeRoleWithWebIdentity',
            'Version' => '2011-06-15',
            'RoleArn' => role_arn,
            'WebIdentityToken' => token,
            'RoleSessionName' => role_session_name
          }
          uri.query = ::URI.encode_www_form(params)
          req = Net::HTTP::Post.new(uri)
          req['Accept'] = 'application/json'
          resp = Net::HTTP.start(uri.hostname, uri.port, use_ssl: true) do |https|
            https.request(req)
          end
          if resp.code != '200'
            return nil
          end
          resp
        rescue Errno::ENOENT, IOError, SystemCallError
          nil
        end

        # Extracts credentials from AssumeRoleWithWebIdentity response.
        #
        # @param [ Net::HTTPResponse ] response AssumeRoleWithWebIdentity
        #   call response.
        #
        # @return [ Auth::Aws::Credentials | nil ] A set of credentials, or nil
        #   if response parsing failed.
        def credentials_from_web_identity_response(response)
          payload = JSON.parse(response.body).dig(
            'AssumeRoleWithWebIdentityResponse',
            'AssumeRoleWithWebIdentityResult',
            'Credentials'
          ) || {}
          Credentials.new(
            payload['AccessKeyId'],
            payload['SecretAccessKey'],
            payload['SessionToken'],
            Time.at(payload['Expiration'])
          )
        rescue JSON::ParserError, TypeError
          nil
        end

        def http_get(http, uri, metadata_token)
          req = Net::HTTP::Get.new(uri,
            {'x-aws-ec2-metadata-token' => metadata_token})
          http.request(req)
        end

        # Checks whether the credentials provided are valid.
        #
        # Returns true if they are valid, false if they are empty, and
        # raises Auth::InvalidConfiguration if the credentials are
        # incomplete (i.e. some of the components are missing).
        def credentials_valid?(credentials, source)
          unless credentials.access_key_id || credentials.secret_access_key ||
            credentials.session_token
          then
            return false
          end

          if credentials.access_key_id || credentials.secret_access_key
            if credentials.access_key_id && !credentials.secret_access_key
              raise Auth::InvalidConfiguration,
                "Access key ID is provided without secret access key (source: #{source})"
            end

            if credentials.secret_access_key && !credentials.access_key_id
              raise Auth::InvalidConfiguration,
                "Secret access key is provided without access key ID (source: #{source})"
            end

          elsif credentials.session_token
            raise Auth::InvalidConfiguration,
              "Session token is provided without access key ID or secret access key (source: #{source})"
          end

          true
        end
      end
    end
  end
end
