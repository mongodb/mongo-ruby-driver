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

    # An authenticator for PLAIN authentication against MongoDB.
    #
    # @since 2.0.0
    class Plain
      include Authenticatable

      private

      # Get a PLAIN authentication login message.
      #
      # @return [ Mongo::Protocol::Query ] Wire protocol message.
      #
      # @since 2.0.0
      def login_message
        payload = BSON::Binary.new("\x00#{@username}\x00#{@opts[:password]}"),
        Mongo::Protocol::Query.new(db_name,
                                   Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   { :saslStart     => 1,
                                     :mechanism     => 'PLAIN',
                                     :payload       => payload,
                                     :autoAuthorize => 1 })
      end

      # Validate credentials for PLAIN authentication.
      #
      # @since 2.0.0
      def validate_credentials
        unless @opts[:password]
          raise ArgumentError,
          "PLAIN authentication requires both a username and password"
        end
      end
    end
  end
end
