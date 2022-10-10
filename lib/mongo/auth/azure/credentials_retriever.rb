# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2022 MongoDB Inc.
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
    module Azure

      # Raised when could not obtain credentials.
      #
      # @api private
      class CredentialsNotFound < Mongo::Error::AuthError; end

      # Retrieves Azure credentials from Azure Instance Metadata Service (IMDS).
      #
      # @api private
      class CredentialsRetriever

        # Timeout for credentiala retrieving operations, in seconds.
        TIMEOUT = 3

        # Create an instance ont the credentials retriever.
        #
        # @param [ String ] imds_host Azure Instance Metadata Service host
        #   to be used for the retrieving the credentials. Defaulted to the
        #   actual value in Azure cloud - https://learn.microsoft.com/en-us/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http
        #   This parameter should be passed only in tests.
        # @param [ String | Integrer ] port Azure Instance Metadata Service port.
        #   This parameter should be passed only in tests.
        # @param [ Hash ] headers Additional headers to be sent with the credentials
        #   request. This parameter should be passed only in tests.
        def initialize(imds_host = '169.254.169.254', imds_port = nil, headers = {})
          @imds_host = imds_host
          @imds_port = imds_port
          @headers = headers
        end

        # Retrieves a valid set of credentials, if possible, or raises
        # CredentialsNotFound.
        #
        # @return [ Auth::Azure::Credentials ] A set of Azure credentials.
        #
        # @raise Auth::Azure::CredentialsNotFound if credentials could not be
        #   retrieved for any reason.
        def credentials
          http = Net::HTTP.new(@imds_host, @imds_port)
          req = Net::HTTP::Get.new(
            '/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://vault.azure.net',
            {
              'Metadata' => 'true'
            }.merge(@headers)
          )
          resp = ::Timeout.timeout(TIMEOUT) do
            http.request(req)
          end
          if resp.code != '200'
            raise CredentialsNotFound, 'Request to Azure IMDS service failed'
          end
          resp_payload = JSON.parse(resp.body)
          missing_keys = ['access_token', 'resource', 'token_type', 'expires_in'] - resp_payload.keys
          if !missing_keys.empty?
            raise CredentialsNotFound,
              "Invalid Azure IMDS response, #{missing_keys.join(', ')} key(s) are missing"
          end
          Azure::Credentials.new(
            access_token: resp_payload['access_token'],
            resource: resp_payload['resource'],
            token_type: resp_payload['token_type'],
            expires_in: resp_payload['expires_in']
          )
        rescue JSON::ParserError => e
          raise CredentialsNotFound, "Invalid Azure IMDS response: #{e.class}: #{e.message}"
        end
      end
    end
  end
end
