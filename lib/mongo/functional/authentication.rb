# Copyright (C) 2009-2013 MongoDB, Inc.
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
    EXTRA             = { 'GSSAPI' => [:gssapi_service_name, :canonicalize_host_name] }

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

        if (auth[:mechanism] == 'MONGODB-CR' || auth[:mechanism] == 'PLAIN') && !auth[:password]
          raise MongoArgumentError,
            "When using the authentication mechanism #{auth[:mechanism]} " +
            "both username and password are required."
        end
        # if extra opts exist, validate them
        allowed_keys = EXTRA[auth[:mechanism]]
        if auth[:extra] && !auth[:extra].empty?
          invalid_opts = []
          auth[:extra].keys.each { |k| invalid_opts << k unless allowed_keys.include?(k) }
          raise MongoArgumentError,
            "Invalid extra option(s): #{invalid_opts} found. Please check the extra options" +
            " passed and try again." unless invalid_opts.empty?
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
    # @param extra [Hash] (nil) A optional hash of extra options to be stored with
    #   the credential set.
    #
    # @raise [MongoArgumentError] Raised if the database has already been used
    #   for authentication. A log out is required before additional auths can
    #   be issued against a given database.
    # @raise [AuthenticationError] Raised if authentication fails.
    # @return [Hash] a hash representing the authentication just added.
    def add_auth(db_name, username, password=nil, source=nil, mechanism=nil, extra=nil)
      auth = Authentication.validate_credentials({
        :db_name   => db_name,
        :username  => username,
        :password  => password,
        :source    => source,
        :mechanism => mechanism,
        :extra     => extra
      })

      if @auths.any? {|a| a[:source] == auth[:source]}
        raise MongoArgumentError,
          "Another user has already authenticated to the database " +
          "'#{auth[:source]}' and multiple authentications are not " +
          "permitted. Please logout first."
      end

      begin
        socket = self.checkout_reader(:mode => :primary_preferred)
        self.issue_authentication(auth, :socket => socket)
      ensure
        socket.checkin if socket
      end

      @auths << auth
      auth
    end

    # Remove a saved authentication for this connection.
    #
    # @param db_name [String] The database name.
    #
    # @return [Boolean] The result of the operation.
    def remove_auth(db_name)
      return false unless @auths
      @auths.reject! { |a| a[:source] == db_name } ? true : false
    end

    # Remove all authentication information stored in this connection.
    #
    # @return [Boolean] result of the operation.
    def clear_auths
      @auths = Set.new
      true
    end

    # Method to handle and issue logout commands.
    #
    # @note This method should not be called directly. Use DB#logout.
    #
    # @param db_name [String] The database name.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @option opts [Socket] socket Socket instance to use.
    #
    # @raise [MongoDBError] Raised if the logout operation fails.
    # @return [Boolean] The result of the logout operation.
    def issue_logout(db_name, opts={})
      doc = auth_command({:logout => 1}, opts[:socket], db_name).first
      unless Support.ok?(doc)
        raise MongoDBError, "Error logging out on DB #{db_name}."
      end
      true # somewhat pointless, but here to preserve the existing API
    end

    # Method to handle and issue authentication commands.
    #
    # @note This method should not be called directly. Use DB#authenticate.
    #
    # @param auth [Hash] The authentication credentials to be used.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @option opts [Socket] socket Socket instance to use.
    #
    # @raise [AuthenticationError] Raised if the authentication fails.
    # @return [Boolean] Result of the authentication operation.
    def issue_authentication(auth, opts={})
      result = case auth[:mechanism]
        when 'MONGODB-CR'
          issue_cr(auth, opts)
        when 'MONGODB-X509'
          issue_x509(auth, opts)
        when 'PLAIN'
          issue_plain(auth, opts)
        when 'GSSAPI'
          issue_gssapi(auth, opts)
      end

      unless Support.ok?(result)
        raise AuthenticationError,
          "Failed to authenticate user '#{auth[:username]}' " +
          "on db '#{auth[:source]}'."
      end

      true
    end

    private

    # Handles issuing authentication commands for the MONGODB-CR auth mechanism.
    #
    # @param auth [Hash] The authentication credentials to be used.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @option opts [Socket] socket Socket instance to use.
    #
    # @return [Boolean] Result of the authentication operation.
    #
    # @private
    def issue_cr(auth, opts={})
      db_name = auth[:source]
      nonce   = get_nonce(auth[:source], opts)

      # build auth command document
      cmd = BSON::OrderedHash.new
      cmd['authenticate'] = 1
      cmd['user'] = auth[:username]
      cmd['nonce'] = nonce
      cmd['key'] = Authentication.auth_key(auth[:username],
                                           auth[:password],
                                           nonce)
      auth_command(cmd, opts[:socket], db_name).first
    end

    # Handles issuing authentication commands for the MONGODB-X509 auth mechanism.
    #
    # @param auth [Hash] The authentication credentials to be used.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @private
    def issue_x509(auth, opts={})
      db_name = '$external'

      cmd = BSON::OrderedHash.new
      cmd[:authenticate] = 1
      cmd[:mechanism]    = auth[:mechanism]
      cmd[:user]         = auth[:username]

      auth_command(cmd, opts[:socket], db_name).first
    end

    # Handles issuing authentication commands for the PLAIN auth mechanism.
    #
    # @param auth [Hash] The authentication credentials to be used.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @option opts [Socket] socket Socket instance to use.
    #
    # @return [Boolean] Result of the authentication operation.
    #
    # @private
    def issue_plain(auth, opts={})
      db_name = auth[:source]
      payload = "\x00#{auth[:username]}\x00#{auth[:password]}"

      cmd = BSON::OrderedHash.new
      cmd[:saslStart]     = 1
      cmd[:mechanism]     = auth[:mechanism]
      cmd[:payload]       = BSON::Binary.new(payload)
      cmd[:autoAuthorize] = 1

      auth_command(cmd, opts[:socket], db_name).first
    end

    # Handles issuing authentication commands for the GSSAPI auth mechanism.
    #
    # @param auth [Hash] The authentication credentials to be used.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @private
    def issue_gssapi(auth, opts={})
      raise "In order to use Kerberos, please add the mongo-kerberos gem to your dependencies"
    end

    # Helper to fetch a nonce value from a given database instance.
    #
    # @param database [Mongo::DB] The DB instance to use for issue the nonce command.
    # @param opts [Hash] Hash of optional settings and configuration values.
    #
    # @option opts [Socket] socket Socket instance to use.
    #
    # @raise [MongoDBError] Raised if there is an error executing the command.
    # @return [String] Returns the nonce value.
    #
    # @private
    def get_nonce(db_name, opts={})
      cmd = BSON::OrderedHash.new
      cmd[:getnonce] = 1
      doc = auth_command(cmd, opts[:socket], db_name).first

      unless Support.ok?(doc)
        raise MongoDBError, "Error retrieving nonce: #{doc}"
      end
      doc['nonce']
    end

    def auth_command(selector, socket, db_name)
      begin
        message        = build_command_message(db_name, selector)
        request_id     = add_message_headers(message, Mongo::Constants::OP_QUERY)
        packed_message = message.to_s

        send_message_on_socket(packed_message, socket)
        receive(socket, request_id).shift
      rescue OperationFailure => ex
        return ex.result
      rescue ConnectionFailure, OperationTimeout => ex
        socket.close
        raise ex
      end
    end
  end
end
