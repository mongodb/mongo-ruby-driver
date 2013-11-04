# Copyright (C) 2013 MongoDB, Inc.
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

require 'digest/md5'

module Mongo
  module Authentication

    DEFAULT_MECHANISM = 'MONGODB-CR'
    MECHANISMS        = ['GSSAPI', 'MONGODB-CR', 'MONGODB-X509', 'PLAIN']

    # authentication module methods
    class << self
      # Helper to validate an authentication mechanism and optionally
      # raise an error if invalid.
      #
      # @param  mechanism [String] [description]
      # @param  raise_error [Boolean] [description]
      #
      # @raise [ArgumentError] if raise_error and not a valid auth mechanism.
      # @return [Boolean] returns the validation result.
      def validate_mechanism(mechanism, raise_error=false)
        return true if MECHANISMS.include?(mechanism.upcase)
        if raise_error
          raise ArgumentError,
            "Invalid authentication mechanism provided. Must be one of " +
            "#{Mongo::Authentication::MECHANISMS.join(', ')}."
        end
        false
      end


      # Helper to validate and normalize credential sets.
      #
      # @param auth [Hash] A hash containing the credential set.
      #
      # @raise [MongoArgumentError] if the credential set is invalid.
      # @return [Hash] The validated credential set.
      def validate_credentials(auth)
        # set the default auth mechanism if not defined
        auth[:mechanism] ||= DEFAULT_MECHANISM

        # set the default auth source if not defined
        auth[:source] = auth[:source] || auth[:db_name] || 'admin'

        if auth[:mechanism] == 'MONGODB-CR' && !auth[:password]
          # require password when using legacy auth
          raise MongoArgumentError,
            'When using the default authentication mechanism (MONGODB-CR) ' +
            'both username and password are required.'
        end
        auth
      end

      # Generate an MD5 for authentication.
      #
      # @param username [String] The username.
      # @param password [String] The user's password.
      # @param nonce [String] The nonce value.
      #
      # @return [String] MD5 key for db authentication.
      def auth_key(username, password, nonce)
        Digest::MD5.hexdigest("#{nonce}#{username}#{hash_password(username, password)}")
      end

      # Return a hashed password for auth.
      #
      # @param username [String] The username.
      # @param password [String] The users's password.
      #
      # @return [String] The hashed password value.
      def hash_password(username, password)
        Digest::MD5.hexdigest("#{username}:mongo:#{password}")
      end
    end

    # Saves a cache of authentication credentials to the current
    # client instance. This method is called automatically by DB#authenticate.
    #
    # @param db_name [String] The current database name.
    # @param username [String] The current username.
    # @param password [String] (nil) The users's password (not required for
    #   all authentication mechanisms).
    # @param source [String] (nil) The authentication source database
    #   (if different than the current database).
    # @param mechanism [String] (nil) The authentication mechanism being used
    #   (default: 'MONGODB-CR').
    #
    # @return [Hash] a hash representing the authentication just added.
    def add_auth(db_name, username, password=nil, source=nil, mechanism=nil)
      auth = Authentication.validate_credentials({
        :db_name   => db_name,
        :username  => username,
        :password  => password,
        :source    => source,
        :mechanism => mechanism
      })

      if @auths.any? {|a| a[:source] == auth[:source]}
        raise MongoArgumentError,
          "Another user has already authenticated to the database " +
          "'#{source}' and multiple authentications are not permitted. " +
          "Please logout first."
      end

      @auths << auth
      auth
    end

    # Remove a saved authentication for this connection.
    #
    # @param [String] The database name.
    #
    # @return [Boolean] The result of the operation.
    def remove_auth(database)
      return unless @auths
      @auths.reject! { |a| a[:source] == database } ? true : false
    end

    # Remove all authentication information stored in this connection.
    #
    # @return [Boolean] result of the operation.
    def clear_auths
      @auths = Set.new
      true
    end

    def authenticate_pools
      @primary_pool.authenticate_existing
    end

    def logout_pools(database)
      @primary_pool.logout_existing(database)
    end

    # Apply each of the saved database authentications.
    #
    # @return [Boolean] returns true if authentications exist and succeeds,
    #   false if none exists.
    #
    # @raise [AuthenticationError] raises an exception if any one
    #   authentication fails.
    def apply_saved_authentication(opts={})
      return false if @auths.empty?
      @auths.each do |auth|
        self[auth[:source]].issue_authentication(
          auth[:username],
          auth[:password],
          false,
          :source => auth[:source],
          :socket => opts[:socket])
      end
      true
    end

  end
end
