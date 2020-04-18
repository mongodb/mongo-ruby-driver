# Copyright (C) 2014-2020 MongoDB Inc.
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
    class SCRAM

      # Defines behavior around a single SCRAM-SHA-1/256 conversation between
      # the client and server.
      #
      # @since 2.0.0
      # @api private
      class Conversation < SaslConversationBase

        # The client key string.
        #
        # @since 2.0.0
        # @deprecated
        CLIENT_KEY = 'Client Key'.freeze

        # The conversation id field.
        #
        # @since 2.0.0
        ID = 'conversationId'.freeze

        # The iterations key in the responses.
        #
        # @since 2.0.0
        ITERATIONS = /i=(\d+)/.freeze

        # The minimum iteration count for SCRAM-SHA-256.
        #
        # @api private
        #
        # @since 2.6.0
        MIN_ITER_COUNT = 4096

        # The payload field.
        #
        # @since 2.0.0
        PAYLOAD = 'payload'.freeze

        # The server nonce key in the responses.
        #
        # @since 2.0.0
        SERVER_NONCE = /r=([^,]*)/.freeze

        # The salt key in the responses.
        #
        # @since 2.0.0
        SALT = /s=([^,]*)/.freeze

        # The server key string.
        #
        # @since 2.0.0
        # @deprecated
        SERVER_KEY = 'Server Key'.freeze

        # The server signature verifier in the response.
        #
        # @since 2.0.0
        VERIFIER = /v=([^,]*)/.freeze

        # @return [ String ] client_nonce The client nonce.
        attr_reader :client_nonce

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
        #
        # @since 2.0.0
        def continue(reply_document, connection)
          @id = reply_document[ID]
          payload_data = reply_document[PAYLOAD].data
          @server_nonce = payload_data.match(SERVER_NONCE)[1]
          @salt = payload_data.match(SALT)[1]
          @iterations = payload_data.match(ITERATIONS)[1].to_i.tap do |i|
            if i < MIN_ITER_COUNT
              raise Error::InsufficientIterationCount.new(
                Error::InsufficientIterationCount.message(MIN_ITER_COUNT, i))
            end
          end
          @auth_message = "#{first_bare},#{payload_data},#{without_proof}"

          validate_server_nonce!

          if connection && connection.features.op_msg_enabled?
            selector = CLIENT_CONTINUE_MESSAGE.merge(
              payload: client_final_message,
              conversationId: id,
            )
            selector[Protocol::Msg::DATABASE_IDENTIFIER] = user.auth_source
            cluster_time = connection.mongos? && connection.cluster_time
            selector[Operation::CLUSTER_TIME] = cluster_time if cluster_time
            Protocol::Msg.new([], {}, selector)
          else
            Protocol::Query.new(
              user.auth_source,
              Database::COMMAND,
              CLIENT_CONTINUE_MESSAGE.merge(
                payload: client_final_message,
                conversationId: id,
              ),
              limit: -1,
            )
          end
        end

        # Finalize the SCRAM conversation. This is meant to be iterated until
        # the provided reply indicates the conversation is finished.
        #
        # @param [ BSON::Document ] reply_document The reply document of the
        #   previous message.
        # @param [ Server::Connection ] connection The connection being authenticated.
        #
        # @return [ Protocol::Query ] The next message to send.
        #
        # @since 2.0.0
        def finalize(reply_document, connection)
          payload_data = reply_document[PAYLOAD].data
          v = payload_data.match(VERIFIER)
          if v
            @verifier = v[1]
          else
            raise Error::MissingScramServerSignature
          end

          if compare_digest(verifier, server_signature)
            @server_verified = true
          else
            raise Error::InvalidSignature.new(verifier, server_signature)
          end

          if connection && connection.features.op_msg_enabled?
            selector = CLIENT_CONTINUE_MESSAGE.merge(
              payload: client_empty_message,
              conversationId: id,
            )
            selector[Protocol::Msg::DATABASE_IDENTIFIER] = user.auth_source
            cluster_time = connection.mongos? && connection.cluster_time
            selector[Operation::CLUSTER_TIME] = cluster_time if cluster_time
            Protocol::Msg.new([], {}, selector)
          else
            Protocol::Query.new(
              user.auth_source,
              Database::COMMAND,
              CLIENT_CONTINUE_MESSAGE.merge(
                payload: client_empty_message,
                conversationId: id,
              ),
              limit: -1,
            )
          end
        end

        def full_mechanism
          MECHANISMS[@mechanism]
        end

        # Get the id of the conversation.
        #
        # @example Get the id of the conversation.
        #   conversation.id
        #
        # @return [ Integer ] The conversation id.
        #
        # @since 2.0.0
        attr_reader :id

        # Create the new conversation.
        #
        # @example Create the new conversation.
        #   Conversation.new(user, mechanism)
        #
        # @param [ Auth::User ] user The user to converse about.
        # @param [ Symbol ] mechanism Authentication mechanism.
        #
        # @since 2.0.0
        def initialize(user, mechanism)
          unless [:scram, :scram256].include?(mechanism)
            raise InvalidMechanism.new(mechanism)
          end

          super(user)
          @client_nonce = SecureRandom.base64
          @mechanism = mechanism
        end

        private

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
          @client_final ||= client_proof(client_key, client_signature(stored_key(client_key), auth_message))
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

        # HI algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-2.2
        #
        # @since 2.0.0
        def hi(data)
          case @mechanism
          when :scram256
            OpenSSL::PKCS5.pbkdf2_hmac(
              data,
              Base64.strict_decode64(salt),
              iterations,
              digest.size,
              digest
            )
          else
            OpenSSL::PKCS5.pbkdf2_hmac_sha1(
              data,
              Base64.strict_decode64(salt),
              iterations,
              digest.size
            )
          end
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

        # Salted password algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-3
        #
        # @since 2.0.0
        def salted_password
          @salted_password ||= CredentialCache.cache(cache_key(:salted_password)) do
            case @mechanism
            when :scram256
              hi(user.sasl_prepped_password)
            else
              hi(user.hashed_password)
            end
          end
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

        # Get the verifier token from the server response.
        #
        # @api private
        #
        # @since 2.0.0
        attr_reader :verifier

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

        private

        def digest
          @digest ||= case @mechanism
                      when :scram256
                        OpenSSL::Digest::SHA256.new.freeze
                      else
                        OpenSSL::Digest::SHA1.new.freeze
                      end
        end
      end
    end
  end
end
