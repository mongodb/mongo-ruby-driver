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
require 'mongo/auth/scram/conversation'

module Mongo
  module Auth
    class SCRAM

      # Defines behaviour around a single SCRAM-SHA-1 conversation between the
      # client and server.
      #
      # @since 2.0.0
      class Conversation

        # The base client continue message.
        #
        # @since 2.0.0
        CLIENT_CONTINUE_MESSAGE = { saslContinue: 1 }.freeze

        # The base client first message.
        #
        # @since 2.0.0
        CLIENT_FIRST_MESSAGE = { saslStart: 1, autoAuthorize: 1 }.freeze

        # The client key string.
        #
        # @since 2.0.0
        CLIENT_KEY = 'Client Key'.freeze

        # The digest to use for encryption.
        #
        # @since 2.0.0
        DIGEST = Krypt::Digest::SHA1.new.freeze

        # The key for the done field in the responses.
        #
        # @since 2.0.0
        DONE = 'done'.freeze

        # The conversation id field.
        #
        # @since 2.0.0
        ID = 'conversationId'.freeze

        # The iterations key in the responses.
        #
        # @since 2.0.0
        ITERATIONS = /i=(\d+)/.freeze

        # The payload field.
        #
        # @since 2.0.0
        PAYLOAD = 'payload'.freeze

        # The rnonce key in the responses.
        #
        # @since 2.0.0
        RNONCE = /r=([^,]*)/.freeze

        # The salt key in the responses.
        #
        # @since 2.0.0
        SALT = /s=([^,]*)/.freeze

        # @return [ String ] nonce The initial user nonce.
        attr_reader :nonce

        # @return [ Protocol::Reply ] reply The current reply in the
        #   conversation.
        attr_reader :reply

        # @return [ User ] user The user for the conversation.
        attr_reader :user

        # Continue the SCRAM conversation. This sends the client final message
        # to the server after setting the reply from the previous server
        # communication.
        #
        # @example Continue the conversation.
        #   conversation.continue(reply)
        #
        # @param [ Protocol::Reply ] reply The reply of the previous
        #   message.
        #
        # @return [ Protocol::Query ] The next message to send.
        #
        # @since 2.0.0
        def continue(reply)
          validate!(reply)
          Protocol::Query.new(
            Database::ADMIN,
            Database::COMMAND,
            CLIENT_CONTINUE_MESSAGE.merge(payload: client_final_message, conversationId: id),
            limit: -1
          )
        end

        # Is the SCRAM conversation finished?
        #
        # @example Is the conversation finished?
        #   conversation.done?
        #
        # @return [ true, false ] If the conversation is done.
        #
        # @since 2.0.0
        def done?
          reply.documents[0][DONE] == true
        end

        # Finalize the SCRAM conversation. This is meant to be iterated until
        # the provided reply indicates the conversation is finished.
        #
        # @example Finalize the conversation.
        #   conversation.finalize(reply)
        #
        # @param [ Protocol::Reply ] reply The reply of the previous
        #   message.
        #
        # @return [ Protocol::Query ] The next message to send.
        #
        # @since 2.0.0
        def finalize(reply)
          validate!(reply)
          Protocol::Query.new(
            Database::ADMIN,
            Database::COMMAND,
            CLIENT_CONTINUE_MESSAGE.merge(payload: client_empty_message, conversationId: id),
            limit: -1
          )
        end

        # Start the SCRAM conversation. This returns the first message that
        # needs to be send to the server.
        #
        # @example Start the conversation.
        #   conversation.start
        #
        # @return [ Protocol::Query ] The first SCRAM conversation message.
        #
        # @since 2.0.0
        def start
          Protocol::Query.new(
            Database::ADMIN,
            Database::COMMAND,
            CLIENT_FIRST_MESSAGE.merge(payload: client_first_message, mechanism: SCRAM::MECHANISM),
            limit: -1
          )
        end

        # Get the id of the conversation.
        #
        # @example Get the id of the conversation.
        #   conversation.id
        #
        # @return [ Integer ] The conversation id.
        #
        # @since 2.0.0
        def id
          reply.documents[0][ID]
        end

        # Create the new conversation.
        #
        # @example Create the new coversation.
        #   Conversation.new(user)
        #
        # @param [ Auth::User ] user The user to converse about.
        #
        # @since 2.0.0
        def initialize(user)
          @user = user
          @nonce = SecureRandom.base64
        end

        private

        def auth_message
          "#{first_bare},#{reply.documents[0][PAYLOAD].data},#{without_proof}"
        end

        def client_empty_message
          BSON::Binary.new('')
        end

        def client_final_message
          BSON::Binary.new("#{without_proof},p=#{client_final}")
        end

        def client_first_message
          BSON::Binary.new("n,,#{first_bare}")
        end

        def client_final
          key = client_key(salted_password)
          signature = client_signature(stored_key(key), auth_message)
          client_proof(key, signature)
        end

        def client_key(password)
          hmac(password, CLIENT_KEY)
        end

        def client_proof(key, signature)
          Base64.strict_encode64(xor(key, signature))
        end

        def client_signature(key, message)
          hmac(key, message)
        end

        def first_bare
          "n=#{user.encoded_name},r=#{nonce}"
        end

        def h(string)
          DIGEST.digest(string)
        end

        def hi(password)
          Krypt::PBKDF2.new(DIGEST).generate(
            password,
            Base64.strict_decode64(salt),
            iterations,
            DIGEST.digest_length
          )
        end

        def hmac(password, key)
          Krypt::HMAC.digest(DIGEST, password, key)
        end

        def iterations
          reply.documents[0][PAYLOAD].data.match(ITERATIONS)[1].to_i
        end

        def rnonce
          reply.documents[0][PAYLOAD].data.match(RNONCE)[1]
        end

        def salt
          reply.documents[0][PAYLOAD].data.match(SALT)[1]
        end

        def salted_password
          hi(user.hashed_password)
        end

        def stored_key(key)
          h(key)
        end

        def validate!(reply)
          raise Unauthorized.new(user) if reply.documents[0]['ok'] != 1
          @reply = reply
        end

        def without_proof
          "c=biws,r=#{rnonce}"
        end

        def xor(first, second)
          first.bytes.zip(second.bytes).map{ |(a,b)| (a ^ b).chr }.join('')
        end
      end
    end
  end
end
