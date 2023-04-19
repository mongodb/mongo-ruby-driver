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

    # Defines behavior for MongoDB-CR authentication.
    #
    # @since 2.0.0
    # @deprecated MONGODB-CR authentication mechanism is deprecated
    #   as of MongoDB 3.6. Support for it in the Ruby driver will be
    #   removed in driver version 3.0. Please use SCRAM instead.
    # @api private
    class CR < Base

      # The authentication mechanism string.
      #
      # @since 2.0.0
      MECHANISM = 'MONGODB-CR'.freeze

      # Log the user in on the current connection.
      #
      # @return [ BSON::Document ] The document of the authentication response.
      def login
        converse_2_step(connection, conversation)
      end
    end
  end
end

require 'mongo/auth/cr/conversation'
