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

require 'mongo/auth/user/view'

module Mongo
  module Auth

    # Represents a user in MongoDB.
    #
    # @since 2.0.0
    class User
      include Loggable

      # @return [ String ] The authorization source, either a database or
      #   external name.
      attr_reader :auth_source

      # @return [ String ] The database the user is created in.
      attr_reader :database

      # @return [ Hash ] The authentication mechanism properties.
      attr_reader :auth_mech_properties

      # @return [ Symbol ] The authorization mechanism.
      attr_reader :mechanism

      # @return [ String ] The username.
      attr_reader :name

      # @return [ String ] The cleartext password.
      attr_reader :password

      # @return [ Array<String> ] roles The user roles.
      attr_reader :roles

      # Loggable requires an options attribute. We don't have any options
      # hence provide this as a stub.
      #
      # @api private
      def options
        {}
      end

      # Determine if this user is equal to another.
      #
      # @example Check user equality.
      #   user == other
      #
      # @param [ Object ] other The object to compare against.
      #
      # @return [ true, false ] If the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        return false unless other.is_a?(User)
        name == other.name && database == other.database && password == other.password
      end

      # Get an authentication key for the user based on a nonce from the
      # server.
      #
      # @example Get the authentication key.
      #   user.auth_key(nonce)
      #
      # @param [ String ] nonce The response from the server.
      #
      # @return [ String ] The authentication key.
      #
      # @since 2.0.0
      def auth_key(nonce)
        Digest::MD5.hexdigest("#{nonce}#{name}#{hashed_password}")
      end

      # Get the UTF-8 encoded name with escaped special characters for use with
      # SCRAM authorization.
      #
      # @example Get the encoded name.
      #   user.encoded_name
      #
      # @return [ String ] The encoded user name.
      #
      # @since 2.0.0
      def encoded_name
        name.encode(BSON::UTF8).gsub('=','=3D').gsub(',','=2C')
      end

      # Get the hash key for the user.
      #
      # @example Get the hash key.
      #   user.hash
      #
      # @return [ String ] The user hash key.
      #
      # @since 2.0.0
      def hash
        [ name, database, password ].hash
      end

      # Get the user's hashed password for SCRAM-SHA-1.
      #
      # @example Get the user's hashed password.
      #   user.hashed_password
      #
      # @return [ String ] The hashed password.
      #
      # @since 2.0.0
      def hashed_password
        unless password
          raise Error::MissingPassword
        end

        @hashed_password ||= Digest::MD5.hexdigest("#{name}:mongo:#{password}").encode(BSON::UTF8)
      end

      # Get the user's stringprepped password for SCRAM-SHA-256.
      #
      # @api private
      def sasl_prepped_password
        unless password
          raise Error::MissingPassword
        end

        @sasl_prepped_password ||= StringPrep.prepare(password,
          StringPrep::Profiles::SASL::MAPPINGS,
          StringPrep::Profiles::SASL::PROHIBITED,
          normalize: true, bidi: true).encode(BSON::UTF8)
      end

      # Create the new user.
      #
      # @example Create a new user.
      #   Mongo::Auth::User.new(options)
      #
      # @param [ Hash ] options The options to create the user from.
      #
      # @option options [ String ] :auth_source The authorization database or
      #   external source.
      # @option options [ String ] :database The database the user is
      #   authorized for.
      # @option options [ String ] :user The user name.
      # @option options [ String ] :password The user's password.
      # @option options [ String ] :pwd Legacy option for the user's password.
      #   If :password and :pwd are both specified, :password takes precedence.
      # @option options [ Symbol ] :auth_mech The authorization mechanism.
      # @option options [ Array<String>, Array<Hash> ] roles The user roles.
      #
      # @since 2.0.0
      def initialize(options)
        @database = options[:database] || Database::ADMIN
        @auth_source = options[:auth_source] || self.class.default_auth_source(options)
        @name = options[:user]
        @password = options[:password] || options[:pwd]
        @mechanism = options[:auth_mech]
        if @mechanism
          # Since the driver must select an authentication class for
          # the specified mechanism, mechanisms that the driver does not
          # know about, and cannot translate to an authentication class,
          # need to be rejected.
          unless @mechanism.is_a?(Symbol)
            # Although we documented auth_mech option as being a symbol, we
            # have not enforced this; warn, reject in lint mode
            if Lint.enabled?
              raise Error::LintError, "Auth mechanism #{@mechanism.inspect} must be specified as a symbol"
            else
              log_warn("Auth mechanism #{@mechanism.inspect} should be specified as a symbol")
              @mechanism = @mechanism.to_sym
            end
          end
          unless Auth::SOURCES.key?(@mechanism)
            raise InvalidMechanism.new(options[:auth_mech])
          end
        end
        @auth_mech_properties = options[:auth_mech_properties] || {}
        @roles = options[:roles] || []
      end

      # Get the specification for the user, used in creation.
      #
      # @example Get the user's specification.
      #   user.spec
      #
      # @return [ Hash ] The user spec.
      #
      # @since 2.0.0
      def spec
        {roles: roles}.tap do |spec|
          if password
            spec[:pwd] = password
          end
        end
      end

      private

      # Generate default auth source based on the URI and options
      #
      # @api private
      def self.default_auth_source(options)
        case options[:auth_mech]
        when :aws, :gssapi, :mongodb_x509
          '$external'
        when :plain
          options[:database] || '$external'
        else
          options[:database] || Database::ADMIN
        end
      end
    end
  end
end
