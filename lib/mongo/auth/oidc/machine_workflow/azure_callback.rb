# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2024 MongoDB Inc.
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
    class Oidc
      class MachineWorkflow
        class AzureCallback
          # The base Azure endpoint
          AZURE_BASE_URI = 'http://169.254.169.254/metadata/identity/oauth2/token'.freeze
          # The Azure headers.
          AZURE_HEADERS = { Metadata: 'true', Accept: 'application/json' }.freeze

          attr_reader :token_resource

          def initialize(auth_mech_properties: {})
            @token_resource = auth_mech_properties[:token_resource]
          end

          # Hits the Azure endpoint in order to get the token.
          #
          # @params [ Integer ] timeout The timeout before cancelling.
          # @params [ Integer ] version The OIDC version number.
          # @params [ String ] username The optional username.
          #
          # @returns [ Hash ] A hash with the access token.
          def execute(timeout:, version:, username: nil)
            query = { resource: token_resource, 'api-version' => '2018-02-01' }
            if username
              query[:client_id] = username
            end
            uri = URI(AZURE_BASE_URI);
            uri.query = ::URI.encode_www_form(query)
            request = Net::HTTP::Get.new(uri, AZURE_HEADERS)
            response = Timeout.timeout(timeout) do
              Net::HTTP.start(uri.hostname, uri.port, use_ssl: false) do |http|
                http.request(request)
              end
            end
            if response.code != '200'
              raise Error::OidcError,
                "Azure metadata host responded with code #{response.code}"
            end
            result = JSON.parse(response.body)
            { access_token: result['access_token'], expires_in: result['expires_in'] }
          end
        end
      end
    end
  end
end
