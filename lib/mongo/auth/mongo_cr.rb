# Copyright (C) 2009-2014 MongoDB, Inc.
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

    # An authenticator for MONGODB-CR authentication
    #
    # @since 2.0.0
    class MongodbCR
      include Authenticatable

      private

      # Validate credentials for MONGODB-CR authentication.
      #
      # @since 2.0.0
      def validate_credentials
        unless @opts[:password]
          raise MongoArgumentError,
          "MONGODB-CR authentication requires both a username and password"
        end
      end

      # Generate a wire protocol message to login with MONGODBCR authentication.
      #
      # @return [ Mongo::Protocol::Query ] login message.
      #
      # @since 2.0.0
      def login_message
        nonce = get_nonce(db(@source))
        Mongo::Protocol::Query.new(db_name,
                                   Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   { :authenticate => 1,
                                     :user         => @username,
                                     :nonce        => nonce,
                                     :key          => auth_key(nonce) })
      end

      # Return a hashed password for authentication.
      #
      # @return [ String ] The hashed password value.
      #
      # @since 2.0.0
      def hashed_password
        Digest::MD5.hexdigest("#{@username}:mongo:#{opts[:password]}")
      end

      # Return an MD5 auth key for this authenticator.
      #
      # @param [ String ] nonce The nonce value.
      #
      # @return [ String ] MD5 key for db authentication.
      #
      # @since 2.0.0
      def auth_key(nonce)
        Digest::MD5.hexdigest("#{nonce}#{@username}#{hashed_password}")
      end
    end
  end
end
