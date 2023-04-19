# frozen_string_literal: true
# rubocop:todo all

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
    class Scram

      # Defines behavior around a single SCRAM-SHA-1 conversation between
      # the client and server.
      #
      # @api private
      class Conversation < ScramConversationBase

        private

        # HI algorithm implementation.
        #
        # @api private
        #
        # @see http://tools.ietf.org/html/rfc5802#section-2.2
        #
        # @since 2.0.0
        def hi(data)
          OpenSSL::PKCS5.pbkdf2_hmac_sha1(
            data,
            salt,
            iterations,
            digest.size,
          )
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
            hi(user.hashed_password)
          end
        end

        def digest
          @digest ||= OpenSSL::Digest::SHA1.new.freeze
        end
      end
    end
  end
end
