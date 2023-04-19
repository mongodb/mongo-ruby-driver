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
  module Auth

    # Defines common behavior around authentication conversations between
    # the client and the server.
    #
    # @api private
    class ScramConversationBase < SaslConversationBase

      # The minimum iteration count for SCRAM-SHA-1 and SCRAM-SHA-256.
      MIN_ITER_COUNT = 4096

      # Create the new conversation.
      #
      # @param [ Auth::User ] user The user to converse about.
      # @param [ String | nil ] client_nonce The client nonce to use.
      #   If this conversation is created for a connection that performed
      #   speculative authentication, this client nonce must be equal to the
      #   client nonce used for speculative authentication; otherwise, the
      #   client nonce must not be specified.
      def initialize(user, connection, client_nonce: nil)
        super
        @client_nonce = client_nonce || SecureRandom.base64
      end

      # @return [ String ] client_nonce The client nonce.
      attr_reader :client_nonce

      # Get the id of the conversation.
      #
      # @example Get the id of the conversation.
      #   conversation.id
      #
      # @return [ Integer ] The conversation id.
      attr_reader :id

      # Whether the client verified the ServerSignature from the server.
      #
      # @see https://jira.mongodb.org/browse/SECURITY-621
      #
      # @return [ true | fase ] Whether the server's signature was verified.
      def server_verified?
        !!@server_verified
      end

      # Continue the SCRAM conversation. This sends the client final message
      # to the server after setting the reply from the previous server
      # communication.
      #
      # @param [ BSON::Document ] reply_document The reply document of the
      #   previous message.
      # @param [ Server::Connection ] connection The connection being
      #   authenticated.
      #
      # @return [ Protocol::Message ] The next message to send.
      def continue(reply_document, connection)
        @id = reply_document['conversationId']
        payload_data = reply_document['payload'].data
        parsed_data = parse_payload(payload_data)
        @server_nonce = parsed_data.fetch('r')
        @salt = Base64.strict_decode64(parsed_data.fetch('s'))
        @iterations = parsed_data.fetch('i').to_i.tap do |i|
          if i < MIN_ITER_COUNT
            raise Error::InsufficientIterationCount.new(
              Error::InsufficientIterationCount.message(MIN_ITER_COUNT, i))
          end
        end
        @auth_message = "#{first_bare},#{payload_data},#{without_proof}"

        validate_server_nonce!

        selector = CLIENT_CONTINUE_MESSAGE.merge(
          payload: client_final_message,
          conversationId: id,
        )
        build_message(connection, user.auth_source, selector)
      end

      # Processes the second response from the server.
      #
      # @param [ BSON::Document ] reply_document The reply document of the
      #   continue response.
      def process_continue_response(reply_document)
        payload_data = parse_payload(reply_document['payload'].data)
        check_server_signature(payload_data)
      end

      # Finalize the SCRAM conversation. This is meant to be iterated until
      # the provided reply indicates the conversation is finished.
      #
      # @param [ Server::Connection ] connection The connection being authenticated.
      #
      # @return [ Protocol::Message ] The next message to send.
      def finalize(connection)
        selector = CLIENT_CONTINUE_MESSAGE.merge(
          payload: client_empty_message,
          conversationId: id,
        )
        build_message(connection, user.auth_source, selector)
      end

      # Returns the hash to provide to the server in the handshake
      # as value of the speculativeAuthenticate key.
      #
      # If the auth mechanism does not support speculative authentication,
      # this method returns nil.
      #
      # @return [ Hash | nil ] Speculative authentication document.
      def speculative_auth_document
        client_first_document.merge(db: user.auth_source)
      end

      private

      # Parses a payload like a=value,b=value2 into a hash like
      # {'a' => 'value', 'b' => 'value2'}.
      #
      # @param [ String ] payload The payload to parse.
      #
      # @return [ Hash ] Parsed key-value pairs.
      def parse_payload(payload)
        Hash[payload.split(',').reject { |v| v == '' }.map do |pair|
          k, v, = pair.split('=', 2)
          if k == ''
            raise Error::InvalidServerAuthResponse, 'Payload malformed: missing key'
          end
          [k, v]
        end]
      end

      def client_first_message_options
        {skipEmptyExchange: true}
      end

      # @see http://tools.ietf.org/html/rfc5802#section-3
      def client_first_payload
        "n,,#{first_bare}"
      end

      # Auth message algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      attr_reader :auth_message

      # Get the empty client message.
      #
      # @api private
      #
      # @since 2.0.0
      def client_empty_message
        BSON::Binary.new('')
      end

      # Get the final client message.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      def client_final_message
        BSON::Binary.new("#{without_proof},p=#{client_final}")
      end

      # Client final implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-7
      #
      # @since 2.0.0
      def client_final
        @client_final ||= client_proof(client_key,
          client_signature(stored_key(client_key),
          auth_message))
      end

      # Looks for field 'v' in payload data, if it is present verifies the
      # server signature. If verification succeeds, sets @server_verified
      # to true. If verification fails, raises InvalidSignature.
      #
      # This method can be called from different conversation steps
      # depending on whether the short SCRAM conversation is used.
      def check_server_signature(payload_data)
        if verifier = payload_data['v']
          if compare_digest(verifier, server_signature)
            @server_verified = true
          else
            raise Error::InvalidSignature.new(verifier, server_signature)
          end
        end
      end

      # Client key algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      def client_key
        @client_key ||= CredentialCache.cache(cache_key(:client_key)) do
          hmac(salted_password, 'Client Key')
        end
      end

      # Client proof algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      def client_proof(key, signature)
        @client_proof ||= Base64.strict_encode64(xor(key, signature))
      end

      # Client signature algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      def client_signature(key, message)
        @client_signature ||= hmac(key, message)
      end

      # First bare implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-7
      #
      # @since 2.0.0
      def first_bare
        @first_bare ||= "n=#{user.encoded_name},r=#{client_nonce}"
      end

      # H algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-2.2
      #
      # @since 2.0.0
      def h(string)
        digest.digest(string)
      end

      # HMAC algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-2.2
      #
      # @since 2.0.0
      def hmac(data, key)
        OpenSSL::HMAC.digest(digest, data, key)
      end

      # Get the iterations from the server response.
      #
      # @api private
      #
      # @since 2.0.0
      attr_reader :iterations

      # Get the data from the returned payload.
      #
      # @api private
      #
      # @since 2.0.0
      attr_reader :payload_data

      # Get the server nonce from the payload.
      #
      # @api private
      #
      # @since 2.0.0
      attr_reader :server_nonce

      # Gets the salt from the server response.
      #
      # @api private
      #
      # @since 2.0.0
      attr_reader :salt

      # @api private
      def cache_key(*extra)
        [user.password, salt, iterations, @mechanism] + extra
      end

      # Server key algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      def server_key
        @server_key ||= CredentialCache.cache(cache_key(:server_key)) do
          hmac(salted_password, 'Server Key')
        end
      end

      # Server signature algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      def server_signature
        @server_signature ||= Base64.strict_encode64(hmac(server_key, auth_message))
      end

      # Stored key algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 2.0.0
      def stored_key(key)
        h(key)
      end

      # Get the without proof message.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-7
      #
      # @since 2.0.0
      def without_proof
        @without_proof ||= "c=biws,r=#{server_nonce}"
      end

      # XOR operation for two strings.
      #
      # @api private
      #
      # @since 2.0.0
      def xor(first, second)
        first.bytes.zip(second.bytes).map{ |(a,b)| (a ^ b).chr }.join('')
      end

      def compare_digest(a, b)
        check = a.bytesize ^ b.bytesize
        a.bytes.zip(b.bytes){ |x, y| check |= x ^ y.to_i }
        check == 0
      end
    end
  end
end
