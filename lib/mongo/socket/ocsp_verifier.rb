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

module Net
  autoload :HTTP, 'net/http'
end

module Mongo
  class Socket

    # https://ruby-doc.org/stdlib/libdoc/openssl/rdoc/OpenSSL/OCSP.html
    #
    # @api private
    class OcspVerifier
      include Loggable

      def initialize(host_name, cert, ca_cert, **opts)
        @host_name = host_name
        @cert = cert
        @ca_cert = ca_cert
        @options = opts
      end

      attr_reader :host_name
      attr_reader :cert
      attr_reader :ca_cert
      attr_reader :options

      def timeout
        options[:timeout] || 5
      end

      # @return [ true | false ] Whether the certificate was verified.
      #
      # @raise [ Error::ServerCertificateRevoked ] If the certificate was
      #   definitively revoked.
      def verify
        # https://tools.ietf.org/html/rfc3546#section-2.3
        # prohibits multiple extensions with the same oid.
        ext = cert.extensions.detect do |ext|
          ext.oid == 'authorityInfoAccess'
        end
        return false unless ext

        # Our test certificates have multiple OCSP URIs.
        uris = ext.value.split("\n").select do |line|
          line.start_with?('OCSP - URI:')
        end.map do |line|
          line.split(':', 2).last
        end
        return false if uris.empty?

        # This synchronized array contains definitive pass/fail responses
        # obtained from the responders. We'll take the first one but due to
        # concurrency multiple responses may be produced and queued.
        queue = Queue.new

        # This synchronized array contains strings, one per responder, that
        # explain why each responder hasn't produced a definitive response.
        # These are concatenated and logged if none of the responders produced
        # a definitive respnose, or if the main thread times out waiting for
        # a definitive response (in which case some of the worker threads'
        # diagnostics may be logged and some may not).
        errors = Queue.new

        cert_id = OpenSSL::OCSP::CertificateId.new(
          cert,
          ca_cert,
          OpenSSL::Digest::SHA1.new,
        )
        req = OpenSSL::OCSP::Request.new
        req.add_certid(cert_id)
        req.add_nonce
        serialized_req = req.to_der

        outstanding_requests = uris.count
        outstanding_requests_lock = Mutex.new

        threads = uris.map do |uri|
          # Explicit lambda so that we can return early in the worker thread,
          # to keep the nesting level down.
          Thread.new &-> do
            begin
              http_response = begin
                uri = URI(uri)
                Net::HTTP.start(uri.hostname, uri.port) do |http|
                  http.post(uri.path, serialized_req,
                    'content-type' => 'application/ocsp-request')
                end
              rescue IOError, SystemCallError => e
                errors << "OCSP request to #{uri} failed: #{e.class}: #{e}"
                return false
              end

              if http_response.code != '200'
                errors << "OCSP request to #{uri} failed with HTTP status code #{http_response.code}: #{http_response.body}"
                return false
              end

              resp = OpenSSL::OCSP::Response.new(http_response.body).basic
              store = OpenSSL::X509::Store.new
              # The CA certificate needs to be both in the store and given
              # to the verify call.
              store.add_cert(ca_cert)
              unless resp.verify([ca_cert], store)
                # Ruby's OpenSSL binding discards error information - see
                # https://github.com/ruby/openssl/issues/395
                errors << "OCSP response from #{uri} failed signature verification; set `OpenSSL.debug = true` to see why"
                return false
              end

              if req.check_nonce(resp) <= 0
                errors << "OCSP response from #{uri} included invalid nonce"
                return false
              end

              if resp.respond_to?(:find_response)
                # Ruby 2.4+
                resp = resp.find_response(cert_id)
              else
                # Ruby 2.3
                found = nil
                resp.status.each do |_cert_id, cert_status, revocation_reason, revocation_time, this_update, next_update, extensions|
                  if _cert_id.cmp(cert_id)
                    found = OpenStruct.new(
                      cert_status: cert_status,
                      certid: _cert_id,
                      next_update: next_update,
                      this_update: this_update,
                      revocation_reason: revocation_reason,
                      revocation_time: revocation_time,
                      extensions: extensions,
                    )
                    class << found
                      # Unlike the stdlib method, this one doesn't accept
                      # any arguments.
                      def check_validity
                        now = Time.now
                        this_update <= now && next_update >= now
                      end
                    end
                    break
                  end
                end
                resp = found
              end

              unless resp
                errors << "OCSP response from #{uri} did not include information about the requested certificate"
                return false
              end

              unless resp.check_validity
                errors << "OCSP response from #{uri} was invalid: this_update was in the future or next_update time has passed"
                return false
              end

              unless [
                OpenSSL::OCSP::V_CERTSTATUS_GOOD,
                OpenSSL::OCSP::V_CERTSTATUS_REVOKED,
              ].include?(resp.cert_status)
                errors << "OCSP response from #{uri} had a non-definitive status: #{resp.cert_status}"
                return false
              end

              queue << [uri, resp]
            ensure
              outstanding_requests_lock.synchronize do
                outstanding_requests -= 1
                if outstanding_requests == 0
                  queue << nil
                end
              end
            end
          end
        end

        resp = begin
          ::Timeout.timeout(timeout) do
            queue.shift
          end
        rescue ::Timeout::Error
          nil
        end

        threads.map(&:kill)
        threads.map(&:join)

        if resp
          uri, status = resp
          if status.cert_status == OpenSSL::OCSP::V_CERTSTATUS_REVOKED
            raise Error::ServerCertificateRevoked, "TLS certificate of '#{host_name}' has been revoked according to '#{uri}' for reason '#{status.revocation_reason}' at '#{status.revocation_time}'"
          end
          true
        else
          reasons = []
          errors.length.times do
            reasons << errors.shift
          end
          if reasons.empty?
            msg = "No responses from responders: #{uris.join(', ')} within #{timeout} seconds"
          else
            msg = "For responders #{uris.join(', ')} with a timeout of #{timeout} seconds: #{reasons.join(', ')}"
          end
          log_warn("TLS certificate of '#{host_name}' could not be definitively verified via OCSP: #{msg}")
          false
        end
      end
    end
  end
end
