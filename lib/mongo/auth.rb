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

require 'mongo/auth/credential_cache'
require 'mongo/auth/stringprep'
require 'mongo/auth/user'
require 'mongo/auth/roles'
require 'mongo/auth/base'
require 'mongo/auth/cr'
require 'mongo/auth/ldap'
require 'mongo/auth/scram'
require 'mongo/auth/x509'

module Mongo

  # This namespace contains all authentication related behavior.
  #
  # @since 2.0.0
  module Auth
    extend self

    # The external database name.
    #
    # @since 2.0.0
    EXTERNAL = '$external'.freeze

    # Constant for the nonce command.
    #
    # @since 2.0.0
    GET_NONCE = { getnonce: 1 }.freeze

    # Constant for the nonce field.
    #
    # @since 2.0.0
    NONCE = 'nonce'.freeze

    # Map the symbols parsed from the URI connection string to strategies.
    #
    # @since 2.0.0
    SOURCES = {
      mongodb_cr: CR,
      mongodb_x509: X509,
      plain: LDAP,
      scram: SCRAM,
      scram256: SCRAM,
    }

    # Get the authorization strategy for the provided auth mechanism.
    #
    # @example Get the strategy.
    #   Auth.get(user)
    #
    # @param [ Auth::User ] user The user object.
    #
    # @return [ CR, X509, LDAP, Kerberos ] The auth strategy.
    #
    # @since 2.0.0
    def get(user)
      mechanism = user.mechanism
      raise InvalidMechanism.new(mechanism) if !SOURCES.has_key?(mechanism)
      SOURCES[mechanism].new(user)
    end

    # Raised when trying to authorize with an invalid configuration
    #
    # @since 2.11.0
    class InvalidConfiguration < Mongo::Error::AuthError; end

    # Raised when trying to get an invalid authorization mechanism.
    #
    # @since 2.0.0
    class InvalidMechanism < InvalidConfiguration

      # Instantiate the new error.
      #
      # @example Instantiate the error.
      #   Mongo::Auth::InvalidMechanism.new(:test)
      #
      # @param [ Symbol ] mechanism The provided mechanism.
      #
      # @since 2.0.0
      def initialize(mechanism)
        known_mechanisms = SOURCES.keys.sort.map do |key|
          key.inspect
        end.join(', ')
        super("#{mechanism.inspect} is invalid, please use one of the following mechanisms: #{known_mechanisms}")
      end
    end

    # Raised when a user is not authorized on a database.
    #
    # @since 2.0.0
    class Unauthorized < Mongo::Error::AuthError

      # Instantiate the new error.
      #
      # @example Instantiate the error.
      #   Mongo::Auth::Unauthorized.new(user)
      #
      # @param [ Mongo::Auth::User ] user The unauthorized user.
      # @param [ String ] used_mechanism Auth mechanism actually used for
      #   authentication. This is a full string like SCRAM-SHA-256.
      # @param [ String ] message The error message returned by the server.
      # @param [ Server ] server The server instance that authentication
      #   was attempted against.
      #
      # @since 2.0.0
      def initialize(user, used_mechanism: nil, message: nil,
        server: nil
      )
        configured_bits = []
        used_bits = [
          "auth source: #{user.auth_source}",
        ]

        if user.mechanism
          configured_bits << "mechanism: #{user.mechanism}"
        end

        if used_mechanism
          used_bits << "used mechanism: #{used_mechanism}"
        end

        if server
          used_bits << "used server: #{server.address} (#{server.status})"
        end

        used_user = if user.mechanism == :mongodb_x509
          'Client certificate'
        else
          "User #{user.name}"
        end

        if configured_bits.empty?
          configured_bits = ''
        else
          configured_bits = " (#{configured_bits.join(', ')})"
        end

        used_bits = " (#{used_bits.join(', ')})"

        msg = "#{used_user}#{configured_bits} is not authorized to access #{user.database}#{used_bits}"
        if message
          msg += ': ' + message
        end
        super(msg)
      end
    end
  end
end
