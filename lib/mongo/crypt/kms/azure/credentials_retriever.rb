# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2019-2021 MongoDB Inc.
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
  module Crypt
    module KMS
      module Azure
        # This class retrieves Azure credentials using Azure
        # metadata host. This should be used when the driver is used on the
        # Azure environment.
        #
        # @api private
        class CredentialsRetriever
          # Default host to obtain Azure metadata.
          DEFAULT_HOST = '169.254.169.254'

          # Fetches Azure credentials from Azure metadata host.
          #
          # @param [Hash] extra_headers Extra headers to be passed to the
          #   request. This is used for testing.
          # @param [String | nil] metadata_host Azure metadata host. This
          #   is used for testing.
          #
          # @return [ KMS::Azure::AccessToken ] Azure access token.
          #
          # @raise [KMS::CredentialsNotFound] If credentials could not be found.
          def self.fetch_access_token(extra_headers: {}, metadata_host: nil)
            uri, req = prepare_request(extra_headers, metadata_host)
            parsed_response = fetch_response(uri, req)
            Azure::AccessToken.new(
              parsed_response.fetch('access_token'),
              Integer(parsed_response.fetch('expires_in'))
            )
          rescue KeyError, ArgumentError => e
            raise KMS::CredentialsNotFound,
                  "Azure metadata response is invalid: '#{parsed_response}'; #{e.class}: #{e.message}"
          end

          # Prepares a request to Azure metadata host.
          #
          # @param [Hash] extra_headers Extra headers to be passed to the
          #   request. This is used for testing.
          # @param [String | nil] metadata_host Azure metadata host. This
          #   is used for testing.
          #
          # @return [Array<URI, Net::HTTP::Get>] URI and request object.
          def self.prepare_request(extra_headers, metadata_host)
            host = metadata_host || DEFAULT_HOST
            host = DEFAULT_HOST if host.empty?
            uri = URI("http://#{host}/metadata/identity/oauth2/token")
            uri.query = ::URI.encode_www_form(
              'api-version' => '2018-02-01',
              'resource' => 'https://vault.azure.net'
            )
            req = Net::HTTP::Get.new(uri)
            req['Metadata'] = 'true'
            req['Accept'] = 'application/json'
            extra_headers.each { |k, v| req[k] = v }
            [uri, req]
          end
          private_class_method :prepare_request

          # Fetches response from Azure metadata host.
          #
          # @param [URI] uri URI to Azure metadata host.
          # @param [Net::HTTP::Get] req Request object.
          #
          # @return [Hash] Parsed response.
          #
          # @raise [KMS::CredentialsNotFound] If cannot fetch response or
          #   response is invalid.
          def self.fetch_response(uri, req)
            resp = do_request(uri, req)
            if resp.code != '200'
              raise KMS::CredentialsNotFound,
                    "Azure metadata host responded with code #{resp.code}"
            end
            JSON.parse(resp.body)
          rescue JSON::ParserError => e
            raise KMS::CredentialsNotFound,
                  "Azure metadata response is invalid: '#{resp.body}'; #{e.class}: #{e.message}"
          end
          private_class_method :fetch_response

          # Performs a request to Azure metadata host.
          #
          # @param [URI] uri URI to Azure metadata host.
          # @param [Net::HTTP::Get] req Request object.
          #
          # @return [Net::HTTPResponse] Response object.
          #
          # @raise [KMS::CredentialsNotFound] If cannot execute request.
          def self.do_request(uri, req)
            ::Timeout.timeout(10) do
              Net::HTTP.start(uri.hostname, uri.port, use_ssl: false) do |http|
                http.request(req)
              end
            end
          rescue ::Timeout::Error, IOError, SystemCallError, SocketError => e
            raise KMS::CredentialsNotFound,
                  "Could not receive Azure metadata response; #{e.class}: #{e.message}"
          end
          private_class_method :do_request
        end
      end
    end
  end
end
