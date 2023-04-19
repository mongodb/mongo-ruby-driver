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

require 'mongo/auth/credential_cache'
require 'mongo/auth/stringprep'
require 'mongo/auth/conversation_base'
require 'mongo/auth/sasl_conversation_base'
require 'mongo/auth/scram_conversation_base'
require 'mongo/auth/user'
require 'mongo/auth/roles'
require 'mongo/auth/base'
require 'mongo/auth/aws'
require 'mongo/auth/cr'
require 'mongo/auth/gssapi'
require 'mongo/auth/ldap'
require 'mongo/auth/scram'
require 'mongo/auth/scram256'
require 'mongo/auth/x509'
require 'mongo/error/read_write_retryable'
require 'mongo/error/labelable'


module Mongo

  # This namespace contains all authentication related behavior.
  #
  # @since 2.0.0
  module Auth
    extend self

    # The external database name.
    #
    # @since 2.0.0
    # @api private
    EXTERNAL = '$external'.freeze

    # Constant for the nonce command.
    #
    # @since 2.0.0
    # @api private
    GET_NONCE = { getnonce: 1 }.freeze

    # Constant for the nonce field.
    #
    # @since 2.0.0
    # @api private
    NONCE = 'nonce'.freeze

    # Map the symbols parsed from the URI connection string to strategies.
    #
    # @note This map is not frozen because when mongo_kerberos is loaded,
    #   it mutates this map by adding the Kerberos authenticator.
    #
    # @since 2.0.0
    SOURCES = {
      aws: Aws,
      gssapi: Gssapi,
      mongodb_cr: CR,
      mongodb_x509: X509,
      plain: LDAP,
      scram: Scram,
      scram256: Scram256,
    }

    # Get an authenticator for the provided user to authenticate over the
    # provided connection.
    #
    # @param [ Auth::User ] user The user to authenticate.
    # @param [ Mongo::Connection ] connection The connection to authenticate over.
    #
    # @option opts [ String | nil ] speculative_auth_client_nonce The client
    #   nonce used in speculative auth on the specified connection that
    #   produced the specified speculative auth result.
    # @option opts [ BSON::Document | nil ] speculative_auth_result The
    #   value of speculativeAuthenticate field of hello response of
    #   the handshake on the specified connection.
    #
    # @return [ Auth::Aws | Auth::CR | Auth::Gssapi | Auth::LDAP |
    #   Auth::Scram | Auth::Scram256 | Auth::X509 ] The authenticator.
    #
    # @since 2.0.0
    # @api private
    def get(user, connection, **opts)
      mechanism = user.mechanism
      raise InvalidMechanism.new(mechanism) if !SOURCES.has_key?(mechanism)
      SOURCES[mechanism].new(user, connection, **opts)
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
      include Error::ReadWriteRetryable
      include Error::Labelable

      # @return [ Integer ] The error code.
      attr_reader :code

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
      # @param [ Integer ] The error code.
      #
      # @since 2.0.0
      def initialize(user, used_mechanism: nil, message: nil,
        server: nil, code: nil
      )
        @code = code

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
