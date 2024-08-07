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
      module GCP
        # This class retrieves GPC credentials using Google Compute Engine
        # metadata host. This should be used when the driver is used on the
        # Google Compute Engine instance.
        #
        # @api private
        class CredentialsRetriever
          METADATA_HOST_ENV = 'GCE_METADATA_HOST'

          DEFAULT_HOST = 'metadata.google.internal'

          # Fetch GCP access token.
          #
          # @param [ CsotTimeoutHolder | nil ] timeout_holder CSOT timeout.
          #
          # @return [ String ] GCP access token.
          #
          # @raise [ KMS::CredentialsNotFound ]
          # @raise [ Error::TimeoutError ]
          def self.fetch_access_token(timeout_holder = nil)
            host = ENV.fetch(METADATA_HOST_ENV) { DEFAULT_HOST }
            uri = URI("http://#{host}/computeMetadata/v1/instance/service-accounts/default/token")
            req = Net::HTTP::Get.new(uri)
            req['Metadata-Flavor'] = 'Google'
            resp = fetch_response(uri, req, timeout_holder)
            if resp.code != '200'
              raise KMS::CredentialsNotFound,
                "GCE metadata host responded with code #{resp.code}"
            end
            parsed_resp = JSON.parse(resp.body)
            parsed_resp.fetch('access_token')
          rescue JSON::ParserError, KeyError => e
            raise KMS::CredentialsNotFound,
              "GCE metadata response is invalid: '#{resp.body}'; #{e.class}: #{e.message}"
            rescue ::Timeout::Error, IOError, SystemCallError, SocketError => e
              raise KMS::CredentialsNotFound,
                    "Could not receive GCP metadata response; #{e.class}: #{e.message}"
          end

          def self.fetch_response(uri, req, timeout_holder)
            timeout_holder&.check_timeout!
            if timeout_holder&.timeout?
              ::Timeout.timeout(timeout_holder.remaining_timeout_sec, Error:TimeoutError) do
                do_fetch(uri, req)
              end
            else
              do_fetch(uri, req)
            end
          end
          private_class_method :fetch_response

          def self.do_fetch(uri, req)
            Net::HTTP.start(uri.hostname, uri.port, use_ssl: false) do |http|
              http.request(req)
            end
          end
          private_class_method :do_fetch
        end
      end
    end
  end
end

