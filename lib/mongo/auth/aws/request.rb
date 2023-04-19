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
  module Auth
    class Aws

      # Helper class for working with AWS requests.
      #
      # The primary purpose of this class is to produce the canonical AWS
      # STS request and calculate the signed headers and signature for it.
      #
      # @api private
      class Request

        # The body of the STS GetCallerIdentity request.
        #
        # This is currently the only request that this class supports making.
        STS_REQUEST_BODY = "Action=GetCallerIdentity&Version=2011-06-15".freeze

        # The timeout, in seconds, to use for validating credentials via STS.
        VALIDATE_TIMEOUT = 10

        # Constructs the request.
        #
        # @note By overriding the time, it is possible to create reproducible
        #   requests (in other words, replay a request).
        #
        # @param [ String ] access_key_id The access key id.
        # @param [ String ] secret_access_key The secret access key.
        # @param [ String ] session_token The session token for temporary
        #   credentials.
        # @param [ String ] host The value of Host HTTP header to use.
        # @param [ String ] server_nonce The server nonce binary string.
        # @param [ Time ] time The time of the request.
        def initialize(access_key_id:, secret_access_key:, session_token: nil,
          host:, server_nonce:, time: Time.now
        )
          @access_key_id = access_key_id
          @secret_access_key = secret_access_key
          @session_token = session_token
          @host = host
          @server_nonce = server_nonce
          @time = time

          %i(access_key_id secret_access_key host server_nonce).each do |arg|
            value = instance_variable_get("@#{arg}")
            if value.nil? || value.empty?
              raise Error::InvalidServerAuthResponse, "Value for '#{arg}' is required"
            end
          end

          if host && host.length > 255
              raise Error::InvalidServerAuthHost, "Value for 'host' is too long: #{@host}"
          end
        end

        # @return [ String ] access_key_id The access key id.
        attr_reader :access_key_id

        # @return [ String ] secret_access_key The secret access key.
        attr_reader :secret_access_key

        # @return [ String ] session_token The session token for temporary
        #   credentials.
        attr_reader :session_token

        # @return [ String ] host The value of Host HTTP header to use.
        attr_reader :host

        # @return [ String ] server_nonce The server nonce binary string.
        attr_reader :server_nonce

        # @return [ Time ] time The time of the request.
        attr_reader :time

        # @return [ String ] formatted_time ISO8601-formatted time of the
        #   request, as would be used in X-Amz-Date header.
        def formatted_time
          @formatted_time ||= @time.getutc.strftime('%Y%m%dT%H%M%SZ')
        end

        # @return [ String ] formatted_date YYYYMMDD formatted date of the request.
        def formatted_date
          formatted_time[0, 8]
        end

        # @return [ String ] region The region of the host, derived from the host.
        def region
          # Common case
          if host == 'sts.amazonaws.com'
            return 'us-east-1'
          end

          if host.start_with?('.')
            raise Error::InvalidServerAuthHost, "Host begins with a period: #{host}"
          end
          if host.end_with?('.')
            raise Error::InvalidServerAuthHost, "Host ends with a period: #{host}"
          end

          parts = host.split('.')
          if parts.any? { |part| part.empty? }
            raise Error::InvalidServerAuthHost, "Host has an empty component: #{host}"
          end

          if parts.length == 1
            'us-east-1'
          else
            parts[1]
          end
        end

        # Returns the scope of the request, per the AWS signature V4 specification.
        #
        # @return [ String ] The scope.
        def scope
          "#{formatted_date}/#{region}/sts/aws4_request"
        end

        # Returns the hash containing the headers of the calculated canonical
        # request.
        #
        # @note Not all of these headers are part of the signed headers list,
        #   the keys of the hash are not necessarily ordered lexicographically,
        #   and the keys may be in any case.
        #
        # @return [ <Hash> ] headers The headers.
        def headers
          headers = {
            'content-length' => STS_REQUEST_BODY.length.to_s,
            'content-type' => 'application/x-www-form-urlencoded',
            'host' => host,
            'x-amz-date' => formatted_time,
            'x-mongodb-gs2-cb-flag' => 'n',
            'x-mongodb-server-nonce' => Base64.encode64(server_nonce).gsub("\n", ''),
          }
          if session_token
            headers['x-amz-security-token'] = session_token
          end
          headers
        end

        # Returns the hash containing the headers of the calculated canonical
        # request that should be signed, in a ready to sign form.
        #
        # The differences between #headers and this method is this method:
        #
        # - Removes any headers that are not to be signed. Per AWS
        #   specifications it should be possible to sign all headers, but
        #   MongoDB server expects only some headers to be signed and will
        #   not form the correct request if other headers are signed.
        # - Lowercases all header names.
        # - Orders the headers lexicographically in the hash.
        #
        # @return [ <Hash> ] headers The headers.
        def headers_to_sign
          headers_to_sign = {}
          headers.keys.sort_by { |k| k.downcase }.each do |key|
            write_key = key.downcase
            headers_to_sign[write_key] = headers[key]
          end
          headers_to_sign
        end

        # Returns semicolon-separated list of names of signed headers, per
        # the AWS signature V4 specification.
        #
        # @return [ String ] The signed header list.
        def signed_headers_string
          headers_to_sign.keys.join(';')
        end

        # Returns the canonical request used during calculation of AWS V4
        # signature.
        #
        # @return [ String ] The canonical request.
        def canonical_request
          headers = headers_to_sign
          serialized_headers = headers.map do |k, v|
            "#{k}:#{v}"
          end.join("\n")
          hashed_payload = Digest::SHA256.new.update(STS_REQUEST_BODY).hexdigest
          "POST\n/\n\n" +
            # There are two newlines after serialized headers because the
            # signature V4 specification treats each header as containing the
            # terminating newline, and there is an additional newline
            # separating headers from the signed header names.
            "#{serialized_headers}\n\n" +
            "#{signed_headers_string}\n" +
            hashed_payload
        end

        # Returns the calculated signature of the canonical request, per
        # the AWS signature V4 specification.
        #
        # @return [ String ] The signature.
        def signature
          hashed_canonical_request = Digest::SHA256.hexdigest(canonical_request)
          string_to_sign = "AWS4-HMAC-SHA256\n" +
            "#{formatted_time}\n" +
            "#{scope}\n" +
            hashed_canonical_request
          # All of the intermediate HMAC operations are not hex-encoded.
          mac = hmac("AWS4#{secret_access_key}", formatted_date)
          mac = hmac(mac, region)
          mac = hmac(mac, 'sts')
          signing_key = hmac(mac, 'aws4_request')
          # Only the final HMAC operation is hex-encoded.
          hmac_hex(signing_key, string_to_sign)
        end

        # Returns the value of the Authorization header, per the AWS
        # signature V4 specification.
        #
        # @return [ String ] Authorization header value.
        def authorization
          "AWS4-HMAC-SHA256 Credential=#{access_key_id}/#{scope}, SignedHeaders=#{signed_headers_string}, Signature=#{signature}"
        end

        # Validates the credentials and the constructed request components
        # by sending a real STS GetCallerIdentity request.
        #
        # @return [ Hash ] GetCallerIdentity result.
        def validate!
          sts_request = Net::HTTP::Post.new("https://#{host}").tap do |req|
            headers.each do |k, v|
              req[k] = v
            end
            req['authorization'] = authorization
            req['accept'] = 'application/json'
            req.body = STS_REQUEST_BODY
          end
          http = Net::HTTP.new(host, 443)
          http.use_ssl = true
          http.start do
            resp = Timeout.timeout(VALIDATE_TIMEOUT, Error::CredentialCheckError, 'GetCallerIdentity request timed out') do
              http.request(sts_request)
            end
            payload = JSON.parse(resp.body)
            if resp.code != '200'
              aws_code = payload.fetch('Error').fetch('Code')
              aws_message = payload.fetch('Error').fetch('Message')
              msg = "Credential check for user #{access_key_id} failed with HTTP status code #{resp.code}: #{aws_code}: #{aws_message}"
              msg += '.' unless msg.end_with?('.')
              msg += " Please check that the credentials are valid, and if they are temporary (i.e. use the session token) that the session token is provided and not expired"
              raise Error::CredentialCheckError, msg
            end
            payload.fetch('GetCallerIdentityResponse').fetch('GetCallerIdentityResult')
          end
        end

        private

        def hmac(key, data)
          OpenSSL::HMAC.digest("SHA256", key, data)
        end

        def hmac_hex(key, data)
          OpenSSL::HMAC.hexdigest("SHA256", key, data)
        end

      end
    end
  end
end
