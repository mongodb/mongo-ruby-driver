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

    # Authenticator for x509
    #
    # @since 2.0.0
    class X509
      include Authenticatable

      private

      # Get an X509 authentication login message.
      #
      # @return [ Mongo::Protocol::Query ] Wire protocol message.
      #
      # @since 2.0.0
      def login_message
        Mongo::Protocol::Query.new(db_name,
                                   Mongo::Operation::COMMAND_COLLECTION_NAME,
                                   { :authenticate => 1,
                                     :mechanism    => 'X509',
                                     :user         => @username })
      end
    end
  end
end
