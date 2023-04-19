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
  class Socket

    # This module caches OCSP responses for their indicated validity time.
    #
    # The key is the CertificateId used for the OCSP request.
    # The value is the SingleResponse.
    #
    # @api private
    module OcspCache
      module_function def set(cert_id, response)
        delete(cert_id)
        responses << response
      end

      # Retrieves a cached SingleResponse for the specified CertificateId.
      #
      # This method may return expired responses if they are revoked.
      # Such responses were valid when they were first received.
      #
      # This method may also return responses that are valid but that may
      # expire by the time caller uses them. The caller should not perform
      # update time checks on the returned response.
      #
      # @return [ OpenSSL::OCSP::SingleResponse ] The previously
      #   retrieved response.
      module_function def get(cert_id)
        resp = responses.detect do |resp|
          resp.certid.cmp(cert_id)
        end
        if resp
          # Only expire responses with good status.
          # Once a certificate is revoked, it should stay revoked forever,
          # hence we should be able to cache revoked responses indefinitely.
          if resp.cert_status == OpenSSL::OCSP::V_CERTSTATUS_GOOD &&
            resp.next_update < Time.now
          then
            responses.delete(resp)
            resp = nil
          end
        end

        # If we have connected to a server and cached the OCSP response for it,
        # and then never connect to that server again, the cached OCSP response
        # is going to remain in memory indefinitely. Periodically remove all
        # expired OCSP responses, not just the ones matching the certificate id
        # we are querying by.
        if rand < 0.01
          responses.delete_if do |resp|
            resp.next_update < Time.now
          end
        end

        resp
      end

      module_function def delete(cert_id)
        responses.delete_if do |resp|
          resp.certid.cmp(cert_id)
        end
      end

      # Clears the driver's OCSP response cache.
      #
      # @note Use Mongo.clear_ocsp_cache from applications instead of invoking
      #   this method directly.
      module_function def clear
        responses.replace([])
      end

      private

      LOCK = Mutex.new

      module_function def responses
        LOCK.synchronize do
          @responses ||= []
        end
      end
    end
  end
end
