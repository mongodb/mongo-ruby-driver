# frozen_string_literal: true
# encoding: utf-8

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
    class Aws < Base
      MECHANISM = 'MONGODB-AWS'.freeze

      # Log the user in on the current connection.
      #
      # @return [ BSON::Document ] The document of the authentication response.
      def login
        converse_2_step(connection, conversation)
      end

      # The AWS credential set.
      #
      # @api private
      Credentials = Struct.new(:access_key_id, :secret_access_key, :session_token)
    end
  end
end

require 'mongo/auth/aws/conversation'
require 'mongo/auth/aws/credentials_retriever'
require 'mongo/auth/aws/request'
