# frozen_string_literal: true
# encoding: utf-8

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

      # Retrieves AWS credentials from a variety of sources.
      #
      # This class provides for AWS credentials retrieval from:
      # - the passed user (which receives the credentials passed to the
      #   client via URI options and Ruby options)
      # - AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN
      #   environment variables (commonly used by AWS SDKs and various tools,
      #   as well as AWS Lambda)
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

        def initialize(user = nil)
          @user = user
        end

        # @return [ Auth::User | nil ] The user object, if one was provided.
        attr_reader :user

        # Retrieves a valid set of credentials, if possible, or raises
        # Auth::InvalidConfiguration.
        #
        # @return [ Auth::Aws::Credentials ] A valid set of credentials.
        #
        # @raise Auth::InvalidConfiguration if credentials could not be
        #   retrieved for any reason, or if a source contains an invalid set
        #   of credentials.
        def credentials
          if user
            credentials = Credentials.new(
              user.name,
              user.password,
              user.auth_mech_properties['aws_session_token'],
            )

            if credentials_valid?(credentials, 'Mongo::Client URI or Ruby options')
              return credentials
            end
          end

          credentials = Credentials.new(
            ENV['AWS_ACCESS_KEY_ID'],
            ENV['AWS_SECRET_ACCESS_KEY'],
            ENV['AWS_SESSION_TOKEN'],
          )

          if credentials_valid?(credentials, 'environment variables')
            return credentials
          end

          credentials = ecs_metadata_credentials

          if credentials && credentials_valid?(credentials, 'ECS task metadata')
            return credentials
          end

          credentials = ec2_metadata_credentials

          if credentials && credentials_valid?(credentials, 'EC2 instance metadata')
            return credentials
          end

          raise Auth::InvalidConfiguration,
            "Could not locate AWS credentials (checked Client URI and Ruby options, environment variables, ECS and EC2 metadata)"
        end

        private

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
          )
        # When trying to use the EC2 metadata endpoint on ECS:
        # Errno::EINVAL: Failed to open TCP connection to 169.254.169.254:80 (Invalid argument - connect(2) for "169.254.169.254" port 80)
        rescue ::Timeout::Error, IOError, SystemCallError
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
          )
        rescue ::Timeout::Error, IOError, SystemCallError
          return nil
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
