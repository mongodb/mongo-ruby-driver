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

module Net
  autoload :HTTP, 'net/http'
end

module Mongo
  class Socket

    # OCSP endpoint verifier.
    #
    # After a TLS connection is established, this verifier inspects the
    # certificate presented by the server, and if the certificate contains
    # an OCSP URI, performs the OCSP status request to the specified URI
    # (following up to 5 redirects) to verify the certificate status.
    #
    # @see https://ruby-doc.org/stdlib/libdoc/openssl/rdoc/OpenSSL/OCSP.html
    #
    # @api private
    class OcspVerifier
      include Loggable

      # @param [ String ] host_name The host name being verified, for
      #   diagnostic output.
      # @param [ OpenSSL::X509::Certificate ] cert The certificate presented by
      #   the server at host_name.
      # @param [ OpenSSL::X509::Certificate ] ca_cert The CA certificate
      #   presented by the server or resolved locally from the server
      #   certificate.
      # @param [ OpenSSL::X509::Store ] cert_store The certificate store to
      #   use for verifying OCSP response. This should be the same store as
      #   used in SSLContext used with the SSLSocket that we are verifying the
      #   certificate for. This must NOT be the CA certificate provided by
      #   the server (i.e. anything taken out of peer_cert) - otherwise the
      #   server would dictate which CA authorities the client trusts.
      def initialize(host_name, cert, ca_cert, cert_store, **opts)
        @host_name = host_name
        @cert = cert
        @ca_cert = ca_cert
        @cert_store = cert_store
        @options = opts
      end

      attr_reader :host_name
      attr_reader :cert
      attr_reader :ca_cert
      attr_reader :cert_store
      attr_reader :options

      def timeout
        options[:timeout] || 5
      end

      # @return [ Array<String> ] OCSP URIs in the specified server certificate.
      def ocsp_uris
        @ocsp_uris ||= begin
          # https://tools.ietf.org/html/rfc3546#section-2.3
          # prohibits multiple extensions with the same oid.
          ext = cert.extensions.detect do |ext|
            ext.oid == 'authorityInfoAccess'
          end

          if ext
            # Our test certificates have multiple OCSP URIs.
            ext.value.split("\n").select do |line|
              line.start_with?('OCSP - URI:')
            end.map do |line|
              line.split(':', 2).last
            end
          else
            []
          end
        end
      end

      def cert_id
        @cert_id ||= OpenSSL::OCSP::CertificateId.new(
          cert,
          ca_cert,
          OpenSSL::Digest::SHA1.new,
        )
      end

      def verify_with_cache
        handle_exceptions do
          return false if ocsp_uris.empty?

          resp = OcspCache.get(cert_id)
          if resp
            return return_ocsp_response(resp)
          end

          resp, errors = do_verify

          if resp
            OcspCache.set(cert_id, resp)
          end

          return_ocsp_response(resp, errors)
        end
      end

      # @return [ true | false ] Whether the certificate was verified.
      #
      # @raise [ Error::ServerCertificateRevoked ] If the certificate was
      #   definitively revoked.
      def verify
        handle_exceptions do
          return false if ocsp_uris.empty?

          resp, errors = do_verify
          return_ocsp_response(resp, errors)
        end
      end

      private

      def do_verify
        # This synchronized array contains definitive pass/fail responses
        # obtained from the responders. We'll take the first one but due to
        # concurrency multiple responses may be produced and queued.
        @resp_queue = Queue.new

        # This synchronized array contains strings, one per responder, that
        # explain why each responder hasn't produced a definitive response.
        # These are concatenated and logged if none of the responders produced
        # a definitive respnose, or if the main thread times out waiting for
        # a definitive response (in which case some of the worker threads'
        # diagnostics may be logged and some may not).
        @resp_errors = Queue.new

        @req = OpenSSL::OCSP::Request.new
        @req.add_certid(cert_id)
        @req.add_nonce
        @serialized_req = @req.to_der

        @outstanding_requests = ocsp_uris.count
        @outstanding_requests_lock = Mutex.new

        threads = ocsp_uris.map do |uri|
          Thread.new do
            verify_one_responder(uri)
          end
        end

        resp = begin
          ::Timeout.timeout(timeout) do
            @resp_queue.shift
          end
        rescue ::Timeout::Error
          nil
        end

        threads.map(&:kill)
        threads.map(&:join)

        [resp, @resp_errors]
      end

      def verify_one_responder(uri)
        original_uri = uri
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
              http.post(path, @serialized_req,
                'content-type' => 'application/ocsp-request')
            end
          rescue IOError, SystemCallError => e
            @resp_errors << "OCSP request to #{report_uri(original_uri, uri)} failed: #{e.class}: #{e}"
            return false
          end

          code = http_response.code.to_i
          if (300..399).include?(code)
            redirected_uri = http_response.header['location']
            uri = ::URI.join(uri, redirected_uri)
            redirect_count += 1
            if redirect_count > 5
              @resp_errors << "OCSP request to #{report_uri(original_uri, uri)} failed: too many redirects (6)"
              return false
            end
            next
          end

          if code >= 400
            @resp_errors << "OCSP request to #{report_uri(original_uri, uri)} failed with HTTP status code #{http_response.code}" + report_response_body(http_response.body)
            return false
          end

          if code != 200
            # There must be a body provided with the response, if one isn't
            # provided the response cannot be verified.
            @resp_errors << "OCSP request to #{report_uri(original_uri, uri)} failed with unexpected HTTP status code #{http_response.code}" + report_response_body(http_response.body)
            return false
          end

          break
        end

        resp = OpenSSL::OCSP::Response.new(http_response.body)
        unless resp.basic
          @resp_errors << "OCSP response from #{report_uri(original_uri, uri)} is #{resp.status}: #{resp.status_string}"
          return false
        end
        resp = resp.basic
        unless resp.verify([ca_cert], cert_store)
          # Ruby's OpenSSL binding discards error information - see
          # https://github.com/ruby/openssl/issues/395
          @resp_errors << "OCSP response from #{report_uri(original_uri, uri)} failed signature verification; set `OpenSSL.debug = true` to see why"
          return false
        end

        if @req.check_nonce(resp) == 0
          @resp_errors << "OCSP response from #{report_uri(original_uri, uri)} included invalid nonce"
          return false
        end

        resp = resp.find_response(cert_id)
        unless resp
          @resp_errors << "OCSP response from #{report_uri(original_uri, uri)} did not include information about the requested certificate"
          return false
        end
        # TODO make a new class instead of patching the stdlib one?
        resp.instance_variable_set('@uri', uri)
        resp.instance_variable_set('@original_uri', original_uri)
        class << resp
          attr_reader :uri, :original_uri
        end

        unless resp.check_validity
          @resp_errors << "OCSP response from #{report_uri(original_uri, uri)} was invalid: this_update was in the future or next_update time has passed"
          return false
        end

        unless [
          OpenSSL::OCSP::V_CERTSTATUS_GOOD,
          OpenSSL::OCSP::V_CERTSTATUS_REVOKED,
        ].include?(resp.cert_status)
          @resp_errors << "OCSP response from #{report_uri(original_uri, uri)} had a non-definitive status: #{resp.cert_status}"
          return false
        end

        # Note this returns the redirected URI
        @resp_queue << resp
      rescue => exc
        Utils.warn_bg_exception("Error performing OCSP verification for '#{host_name}' via '#{uri}'", exc,
          logger: options[:logger],
          log_prefix: options[:log_prefix],
          bg_error_backtrace: options[:bg_error_backtrace],
        )
        false
      ensure
        @outstanding_requests_lock.synchronize do
          @outstanding_requests -= 1
          if @outstanding_requests == 0
            @resp_queue << nil
          end
        end
      end

      def return_ocsp_response(resp, errors = nil)
        if resp
          if resp.cert_status == OpenSSL::OCSP::V_CERTSTATUS_REVOKED
            raise_revoked_error(resp)
          end
          true
        else
          reasons = []
          errors.length.times do
            reasons << errors.shift
          end
          if reasons.empty?
            msg = "No responses from responders: #{ocsp_uris.join(', ')} within #{timeout} seconds"
          else
            msg = "For responders #{ocsp_uris.join(', ')} with a timeout of #{timeout} seconds: #{reasons.join(', ')}"
          end
          log_warn("TLS certificate of '#{host_name}' could not be definitively verified via OCSP: #{msg}")
          false
        end
      end

      def handle_exceptions
        begin
          yield
        rescue Error::ServerCertificateRevoked
          raise
        rescue => exc
          Utils.warn_bg_exception(
            "Error performing OCSP verification for '#{host_name}'",
            exc,
            **options)
          false
        end
      end

      def raise_revoked_error(resp)
        if resp.uri == resp.original_uri
          redirect = ''
        else
          redirect = " (redirected from #{resp.original_uri})"
        end
        raise Error::ServerCertificateRevoked, "TLS certificate of '#{host_name}' has been revoked according to '#{resp.uri}'#{redirect} for reason '#{resp.revocation_reason}' at '#{resp.revocation_time}'"
      end

      def report_uri(original_uri, uri)
        if URI(uri) == URI(original_uri)
          uri
        else
          "#{original_uri} (redirected to #{uri})"
        end
      end

      def report_response_body(body)
        if body
          ": #{body}"
        else
          ''
        end
      end
    end
  end
end
