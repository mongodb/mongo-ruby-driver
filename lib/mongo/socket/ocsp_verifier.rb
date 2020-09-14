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
          original_uri = uri
          Thread.new &-> do
            begin
              redirect_count = 0
              http_response = nil
              loop do
                http_response = begin
                  uri = URI(uri)
                  Net::HTTP.start(uri.hostname, uri.port) do |http|
                    path = uri.path
                    if path.empty?
                      path = '/'
                    end
                    http.post(path, serialized_req,
                      'content-type' => 'application/ocsp-request')
                  end
                rescue IOError, SystemCallError => e
                  errors << "OCSP request to #{report_uri(original_uri, uri)} failed: #{e.class}: #{e}"
                  return false
                end

                code = http_response.code.to_i
                if (300..399).include?(code)
                  redirected_uri = http_response.header['location']
                  uri = ::URI.join(uri, redirected_uri)
                  redirect_count += 1
                  if redirect_count > 5
                    errors << "OCSP request to #{report_uri(original_uri, uri)} failed: too many redirects (6)"
                    return false
                  end
                  next
                end

                if http_response.code != '200'
                  errors << "OCSP request to #{report_uri(original_uri, uri)} failed with HTTP status code #{http_response.code}: #{http_response.body}"
                  return false
                end

                break
              end

              resp = OpenSSL::OCSP::Response.new(http_response.body).basic
              store = OpenSSL::X509::Store.new
              # The CA certificate needs to be both in the store and given
              # to the verify call.
              store.add_cert(ca_cert)
              unless resp.verify([ca_cert], store)
                # Ruby's OpenSSL binding discards error information - see
                # https://github.com/ruby/openssl/issues/395
                errors << "OCSP response from #{report_uri(original_uri, uri)} failed signature verification; set `OpenSSL.debug = true` to see why"
                return false
              end

              if req.check_nonce(resp) <= 0
                errors << "OCSP response from #{report_uri(original_uri, uri)} included invalid nonce"
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
                errors << "OCSP response from #{report_uri(original_uri, uri)} did not include information about the requested certificate"
                return false
              end

              unless resp.check_validity
                errors << "OCSP response from #{report_uri(original_uri, uri)} was invalid: this_update was in the future or next_update time has passed"
                return false
              end

              unless [
                OpenSSL::OCSP::V_CERTSTATUS_GOOD,
                OpenSSL::OCSP::V_CERTSTATUS_REVOKED,
              ].include?(resp.cert_status)
                errors << "OCSP response from #{report_uri(original_uri, uri)} had a non-definitive status: #{resp.cert_status}"
                return false
              end

              # Note this returns the redirected URI
              queue << [uri, original_uri, resp]
            rescue => exc
              Utils.warn_bg_exception("Error performing OCSP verification for '#{host_name}' via '#{uri}'", exc,
                logger: options[:logger],
                log_prefix: options[:log_prefix],
                bg_error_backtrace: options[:bg_error_backtrace],
              )
              false
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
          uri, original_uri, status = resp
          if status.cert_status == OpenSSL::OCSP::V_CERTSTATUS_REVOKED
            if uri == original_uri
              redirect = ''
            else
              redirect = " (redirected from #{original_uri})"
            end
            raise Error::ServerCertificateRevoked, "TLS certificate of '#{host_name}' has been revoked according to '#{uri}'#{redirect} for reason '#{status.revocation_reason}' at '#{status.revocation_time}'"
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
      rescue Error::ServerCertificateRevoked
        raise
      rescue => exc
        Utils.warn_bg_exception("Error performing OCSP verification for '#{host_name}'", exc,
          logger: options[:logger],
          log_prefix: options[:log_prefix],
          bg_error_backtrace: options[:bg_error_backtrace],
        )
        false
      end

      def report_uri(original_uri, uri)
        if URI(uri) == URI(original_uri)
          uri
        else
          "#{original_uri} (redirected to #{uri})"
        end
      end
    end
  end
end
