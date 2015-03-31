# Copyright (C) 2014 MongoDB Inc.
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

require 'base64'
require 'securerandom'
require 'openssl'
require 'digest/md5'

module Mongo
  module Authentication

    # Defines behaviour around a single SCRAM-SHA-1 conversation between the
    # client and server.
    #
    # @since 1.12.0
    class SCRAM

      # The client key string.
      #
      # @since 1.12.0
      CLIENT_KEY = 'Client Key'.freeze

      # The digest to use for encryption.
      #
      # @since 1.12.0
      DIGEST = OpenSSL::Digest::SHA1.new.freeze

      # The key for the done field in the responses.
      #
      # @since 1.12.0
      DONE = 'done'.freeze

      # The conversation id field.
      #
      # @since 1.12.0
      ID = 'conversationId'.freeze

      # The iterations key in the responses.
      #
      # @since 1.12.0
      ITERATIONS = /i=(\d+)/.freeze

      # The payload field.
      #
      # @since 1.12.0
      PAYLOAD = 'payload'.freeze

      # The rnonce key in the responses.
      #
      # @since 1.12.0
      RNONCE = /r=([^,]*)/.freeze

      # The salt key in the responses.
      #
      # @since 1.12.0
      SALT = /s=([^,]*)/.freeze

      # The server key string.
      #
      # @since 1.12.0
      SERVER_KEY = 'Server Key'.freeze

      # The server signature verifier in the response.
      #
      # @since 1.12.0
      VERIFIER = /v=([^,]*)/.freeze

      # @return [ String ] nonce The initial user nonce.
      attr_reader :nonce

      # @return [ BSON::OrderedHash ] reply The current reply in the conversation.
      attr_reader :reply

      # @return [ Hash ] auth The authentication details.
      attr_reader :auth

      # @return [ String ] hashed_password The user's hashed password
      attr_reader :hashed_password

      # Continue the SCRAM conversation. This sends the client final message
      # to the server after setting the reply from the previous server
      # communication.
      #
      # @example Continue the conversation.
      #   conversation.continue(reply)
      #
      # @param [ BSON::OrderedHash ] reply The reply of the previous
      #   message.
      #
      # @return [ BSON::OrderedHash ] The next message to send.
      #
      # @since 1.12.0
      def continue(reply)
        validate_first_message!(reply)
        command = BSON::OrderedHash.new
        command['saslContinue'] = 1
        command[PAYLOAD] = client_final_message
        command[ID] = id
        command
      end

      # Continue the SCRAM conversation for copydb. This sends the client final message
      # to the server after setting the reply from the previous server
      # communication.
      #
      # @example Continue the conversation when copying a database.
      #   conversation.copy_db_continue(reply)
      #
      # @param [ BSON::OrderedHash ] reply The reply of the previous
      #   message.
      #
      # @return [ BSON::OrderedHash ] The next message to send.
      #
      # @since 1.12.0
      def copy_db_continue(reply)
        validate_first_message!(reply)
        command = BSON::OrderedHash.new
        command['copydb'] = 1
        command['fromhost'] = @copy_db[:from_host]
        command['fromdb'] = @copy_db[:from_db]
        command['todb'] = @copy_db[:to_db]
        command[PAYLOAD] = client_final_message
        command[ID] = id
        command
      end

      # Finalize the SCRAM conversation. This is meant to be iterated until
      # the provided reply indicates the conversation is finished.
      #
      # @example Finalize the conversation.
      #   conversation.finalize(reply)
      #
      # @param [ BSON::OrderedHash ] reply The reply of the previous
      #   message.
      #
      # @return [ BSON::OrderedHash ] The next message to send.
      #
      # @since 1.12.0
      def finalize(reply)
        validate_final_message!(reply)
        command = BSON::OrderedHash.new
        command['saslContinue'] = 1
        command[PAYLOAD] = client_empty_message
        command[ID] = id
        command
      end

      # Start the SCRAM conversation. This returns the first message that
      # needs to be send to the server.
      #
      # @example Start the conversation.
      #   conversation.start
      #
      # @return [ BSON::OrderedHash ] The first SCRAM conversation message.
      #
      # @since 1.12.0
      def start
        command = BSON::OrderedHash.new
        command['saslStart'] = 1
        command['autoAuthorize'] = 1
        command[PAYLOAD] = client_first_message
        command['mechanism'] = 'SCRAM-SHA-1'
        command
      end

      # Start the SCRAM conversation for copying a database.
      # This returns the first message that needs to be sent to the server.
      #
      # @example Start the copydb conversation.
      #   conversation.copy_db_start
      #
      # @return [ BSON::OrderedHash ] The first SCRAM copy_db conversation message.
      #
      # @since 1.12.0
      def copy_db_start
        command = BSON::OrderedHash.new
        command['copydbsaslstart'] = 1
        command['autoAuthorize'] = 1
        command['fromhost'] = @copy_db[:from_host]
        command['fromdb'] = @copy_db[:from_db]
        command[PAYLOAD] = client_first_message
        command['mechanism'] = 'SCRAM-SHA-1'
        command
      end

      # Get the id of the conversation.
      #
      # @example Get the id of the conversation.
      #   conversation.id
      #
      # @return [ Integer ] The conversation id.
      #
      # @since 1.12.0
      def id
        reply[ID]
      end

      # Create the new conversation.
      #
      # @example Create the new conversation.
      #   Conversation.new(auth, password)
      #
      # @since 1.12.0
      def initialize(auth, hashed_password, opts={})
        @auth = auth
        @hashed_password = hashed_password
        @nonce = SecureRandom.base64
        @copy_db = opts[:copy_db] if opts[:copy_db]
      end

      private

      # Auth message algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def auth_message
        @auth_message ||= "#{first_bare},#{payload_data},#{without_proof}"
      end

      # Get the empty client message.
      #
      # @api private
      #
      # @since 1.12.0
      def client_empty_message
        BSON::Binary.new('')
      end

      # Get the final client message.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def client_final_message
        BSON::Binary.new("#{without_proof},p=#{client_final}")
      end

      # Get the client first message
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def client_first_message
        BSON::Binary.new("n,,#{first_bare}")
      end

      # Client final implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-7
      #
      # @since 1.12.0
      def client_final
        @client_final ||= client_proof(client_key, client_signature(stored_key(client_key), auth_message))
      end

      # Client key algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def client_key
        @client_key ||= hmac(salted_password, CLIENT_KEY)
      end

      if Base64.respond_to?(:strict_encode64)

        # Client proof algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-3
        #
        # @since 1.12.0
        def client_proof(key, signature)
          @client_proof ||= Base64.strict_encode64(xor(key, signature))
        end
      else

        # Client proof algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-3
        #
        # @since 1.12.0
        def client_proof(key, signature)
          @client_proof ||= Base64.encode64(xor(key, signature)).gsub("\n",'')
        end
      end

      # Client signature algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def client_signature(key, message)
        @client_signature ||= hmac(key, message)
      end

      if Base64.respond_to?(:strict_decode64)

        # Get the base 64 decoded salt.
        #
        # @api private
        #
        # @since 1.12.0
        def decoded_salt
          @decoded_salt ||= Base64.strict_decode64(salt)
        end
      else

        # Get the base 64 decoded salt.
        #
        # @api private
        #
        # @since 1.12.0
        def decoded_salt
          @decoded_salt ||= Base64.decode64(salt)
        end
      end

      # First bare implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-7
      #
      # @since 1.12.0
      def first_bare
        @first_bare ||= "n=#{auth[:username].gsub('=','=3D').gsub(',','=2C')},r=#{nonce}"
      end

      # H algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-2.2
      #
      # @since 1.12.0
      def h(string)
        DIGEST.digest(string)
      end

      if defined?(OpenSSL::PKCS5)

        # HI algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-2.2
        #
        # @since 1.12.0
        def hi(data)
          OpenSSL::PKCS5.pbkdf2_hmac_sha1(data, decoded_salt, iterations, DIGEST.size)
        end
      else

        # HI algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-2.2
        #
        # @since 1.12.0
        def hi(data)
          u = hmac(data, decoded_salt + [1].pack("N"))
          v = u
          2.upto(iterations) do |i|
            u = hmac(data, u)
            v = xor(v, u)
          end
          v
        end
      end

      # HMAC algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-2.2
      #
      # @since 1.12.0
      def hmac(data, key)
        OpenSSL::HMAC.digest(DIGEST, data, key)
      end

      # Get the iterations from the server response.
      #
      # @api private
      #
      # @since 1.12.0
      def iterations
        @iterations ||= payload_data.match(ITERATIONS)[1].to_i
      end

      # Get the data from the returned payload.
      #
      # @api private
      #
      # @since 1.12.0
      def payload_data
        reply[PAYLOAD].to_s
      end

      # Get the server nonce from the payload.
      #
      # @api private
      #
      # @since 1.12.0
      def rnonce
        @rnonce ||= payload_data.match(RNONCE)[1]
      end

      # Gets the salt from the server response.
      #
      # @api private
      #
      # @since 1.12.0
      def salt
        @salt ||= payload_data.match(SALT)[1]
      end

      # Salted password algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def salted_password
        @salted_password ||= hi(hashed_password)
      end

      # Server key algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def server_key
        @server_key ||= hmac(salted_password, SERVER_KEY)
      end

      if Base64.respond_to?(:strict_encode64)

        # Server signature algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-3
        #
        # @since 1.12.0
        def server_signature
          @server_signature ||= Base64.strict_encode64(hmac(server_key, auth_message))
        end
      else

        # Server signature algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-3
        #
        # @since 1.12.0
        def server_signature
          @server_signature ||= Base64.encode64(hmac(server_key, auth_message)).gsub("\n",'')
        end
      end

      # Stored key algorithm implementation.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-3
      #
      # @since 1.12.0
      def stored_key(key)
        h(key)
      end

      # Get the verifier token from the server response.
      #
      # @api private
      #
      # @since 1.12.0
      def verifier
        @verifier ||= payload_data.match(VERIFIER)[1]
      end

      # Get the without proof message.
      #
      # @api private
      #
      # @see http://tools.ietf.org/html/rfc5802#section-7
      #
      # @since 1.12.0
      def without_proof
        @without_proof ||= "c=biws,r=#{rnonce}"
      end

      # XOR operation for two strings.
      #
      # @api private
      #
      # @since 1.12.0
      def xor(first, second)
        first.bytes.zip(second.bytes).map{ |(a,b)| (a ^ b).chr }.join('')
      end

      def validate_final_message!(reply)
        validate!(reply)
        unless verifier == server_signature
          raise InvalidSignature.new(verifier, server_signature)
        end
      end

      def validate_first_message!(reply)
        validate!(reply)
        raise InvalidNonce.new(nonce, rnonce) unless rnonce.start_with?(nonce)
      end

      def validate!(reply)
        unless Support.ok?(reply)
          raise AuthenticationError, "Could not authorize user #{auth[:username]} on database #{auth[:db_name]}."
        end
        @reply = reply
      end
    end
  end
end
