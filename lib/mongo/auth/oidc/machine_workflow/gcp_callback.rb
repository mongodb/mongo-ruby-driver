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
        class GcpCallback
          # The base GCP endpoint
          GCP_BASE_URI = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity'.freeze
          # The GCP headers.
          GCP_HEADERS = { 'Metadata-Flavor': 'Google' }.freeze

          attr_reader :token_resource

          # Initialize the Gcp callback.
          #
          # @params [ Hash ] auth_mech_properties The auth mech properties.
          def initialize(auth_mech_properties: {})
            @token_resource = auth_mech_properties[:token_resource]
          end

          # Hits the GCP endpoint in order to get the token. The token_resource will
          # become the audience parameter in the URI.
          #
          # @params [ Integer ] timeout The timeout before cancelling.
          # @params [ Integer ] version The OIDC version number.
          # @params [ String ] username The optional username.
          #
          # @returns [ Hash ] A hash with the access token.
          def execute(timeout:, version:, username: nil)
            uri = URI(GCP_BASE_URI);
            uri.query = ::URI.encode_www_form({ audience: token_resource })
            request = Net::HTTP::Get.new(uri, GCP_HEADERS)
            response = Timeout.timeout(timeout) do
              Net::HTTP.start(uri.hostname, uri.port, use_ssl: false) do |http|
                http.request(request)
              end
            end
            if response.code != '200'
              raise Error::OidcError,
                "GCP metadata host responded with code #{response.code}"
            end
            { access_token: response.body }
          end
        end
      end
    end
  end
end
