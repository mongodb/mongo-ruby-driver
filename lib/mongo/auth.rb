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

require 'mongo/auth/executable'
require 'mongo/auth/cr'
require 'mongo/auth/ldap'
require 'mongo/auth/user'
require 'mongo/auth/x509'

module Mongo

  # This namespace contains all authentication related behaviour.
  #
  # @since 2.0.0
  module Auth

    # Constant for the nonce command.
    #
    # @since 2.0.0
    GET_NONCE = { getnonce: 1 }.freeze

    # Constant for the logout command.
    #
    # @since 2.0.0
    LOGOUT = { logout: 1 }.freeze

    # Constant for the nonce field.
    #
    # @since 2.0.0
    NONCE = 'nonce'.freeze

    class Unauthorized < RuntimeError

      # @return [ Mongo::Auth::User ] The user that was unauthorized.
      attr_reader :user

      # Instantiate the new error.
      #
      # @example Instantiate the error.
      #   Mongo::Auth::Unauthorized.new(user)
      #
      # @param [ Mongo::Auth::User ] user The unauthorized user.
      #
      # @since 2.0.0
      def initialize(user)
        super("User #{user.name} is not authorized to access #{user.database}.")
      end
    end
  end
end
