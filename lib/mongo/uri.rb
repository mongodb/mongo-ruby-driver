# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  # The URI class provides a way for users to parse the MongoDB uri as
  # defined in the connection string format spec.
  #
  # https://www.mongodb.com/docs/manual/reference/connection-string/
  #
  # @example Use the uri string to make a client connection.
  #   uri = Mongo::URI.new('mongodb://localhost:27017')
  #   client = Mongo::Client.new(uri.servers, uri.options)
  #   client.login(uri.credentials)
  #   client[uri.database]
  #
  # @since 2.0.0
  class URI
    include Loggable
    include Address::Validator

    # The uri parser object options.
    #
    # @since 2.0.0
    attr_reader :options

    # Mongo::Options::Redacted of the options specified in the uri.
    #
    # @since 2.1.0
    attr_reader :uri_options

    # The servers specified in the uri.
    #
    # @since 2.0.0
    attr_reader :servers

    # The mongodb connection string scheme.
    #
    # @deprecated Will be removed in 3.0.
    #
    # @since 2.0.0
    SCHEME = 'mongodb://'.freeze

    # The mongodb connection string scheme root.
    #
    # @since 2.5.0
    MONGODB_SCHEME = 'mongodb'.freeze

    # The mongodb srv protocol connection string scheme root.
    #
    # @since 2.5.0
    MONGODB_SRV_SCHEME = 'mongodb+srv'.freeze

    # Error details for an invalid scheme.
    #
    # @since 2.1.0
    # @deprecated
    INVALID_SCHEME = "Invalid scheme. Scheme must be '#{MONGODB_SCHEME}' or '#{MONGODB_SRV_SCHEME}'".freeze

    # MongoDB URI format specification.
    #
    # @since 2.0.0
    FORMAT = 'mongodb://[username:password@]host1[:port1][,host2[:port2]' +
        ',...[,hostN[:portN]]][/[database][?options]]'.freeze

    # MongoDB URI (connection string) documentation url
    #
    # @since 2.0.0
    HELP = 'https://www.mongodb.com/docs/manual/reference/connection-string/'.freeze

    # Unsafe characters that must be urlencoded.
    #
    # @since 2.1.0
    UNSAFE = /[\:\/\@]/

    # Percent sign that must be encoded in user creds.
    #
    # @since 2.5.1
    PERCENT_CHAR = /\%/

    # Unix socket suffix.
    #
    # @since 2.1.0
    UNIX_SOCKET = /.sock/

    # The character delimiting hosts.
    #
    # @since 2.1.0
    HOST_DELIM = ','.freeze

    # The character separating a host and port.
    #
    # @since 2.1.0
    HOST_PORT_DELIM = ':'.freeze

    # The character delimiting a database.
    #
    # @since 2.1.0
    DATABASE_DELIM = '/'.freeze

    # The character delimiting options.
    #
    # @since 2.1.0
    URI_OPTS_DELIM = '?'.freeze

    # The character delimiting multiple options.
    #
    # @since 2.1.0
    # @deprecated
    INDIV_URI_OPTS_DELIM = '&'.freeze

    # The character delimiting an option and its value.
    #
    # @since 2.1.0
    URI_OPTS_VALUE_DELIM = '='.freeze

    # The character separating a username from the password.
    #
    # @since 2.1.0
    AUTH_USER_PWD_DELIM = ':'.freeze

    # The character delimiting auth credentials.
    #
    # @since 2.1.0
    AUTH_DELIM = '@'.freeze

    # Scheme delimiter.
    #
    # @since 2.5.0
    SCHEME_DELIM = '://'.freeze

    # Error details for an invalid options format.
    #
    # @since 2.1.0
    INVALID_OPTS_VALUE_DELIM = "Options and their values must be delimited" +
      " by '#{URI_OPTS_VALUE_DELIM}'".freeze

    # Error details for an non-urlencoded user name or password.
    #
    # @since 2.1.0
    UNESCAPED_USER_PWD = "User name and password must be urlencoded.".freeze

    # Error details for a non-urlencoded unix socket path.
    #
    # @since 2.1.0
    UNESCAPED_UNIX_SOCKET = "UNIX domain sockets must be urlencoded.".freeze

    # Error details for a non-urlencoded auth database name.
    #
    # @since 2.1.0
    UNESCAPED_DATABASE = "Auth database must be urlencoded.".freeze

    # Error details for providing options without a database delimiter.
    #
    # @since 2.1.0
    INVALID_OPTS_DELIM = "Database delimiter '#{DATABASE_DELIM}' must be present if options are specified.".freeze

    # Error details for a missing host.
    #
    # @since 2.1.0
    INVALID_HOST = "Missing host; at least one must be provided.".freeze

    # Error details for an invalid port.
    #
    # @since 2.1.0
    INVALID_PORT = "Invalid port. Port must be an integer greater than 0 and less than 65536".freeze

    # Map of URI read preference modes to Ruby driver read preference modes
    #
    # @since 2.0.0
    READ_MODE_MAP = {
      'primary'            => :primary,
      'primarypreferred'   => :primary_preferred,
      'secondary'          => :secondary,
      'secondarypreferred' => :secondary_preferred,
      'nearest'            => :nearest
    }.freeze

    # Map of URI authentication mechanisms to Ruby driver mechanisms
    #
    # @since 2.0.0
    AUTH_MECH_MAP = {
      'GSSAPI'       => :gssapi,
      'MONGODB-AWS'  => :aws,
      # MONGODB-CR is deprecated and will be removed in driver version 3.0
      'MONGODB-CR'   => :mongodb_cr,
      'MONGODB-X509' => :mongodb_x509,
      'PLAIN'        => :plain,
      'SCRAM-SHA-1'  => :scram,
      'SCRAM-SHA-256' => :scram256,
    }.freeze

    # Options that are allowed to appear more than once in the uri.
    #
    # In order to follow the URI options spec requirement that all instances
    # of 'tls' and 'ssl' have the same value, we need to keep track of all
    # of the values passed in for those options. Assuming they don't conflict,
    # they will be condensed to a single value immediately after parsing the URI.
    #
    # @since 2.1.0
    REPEATABLE_OPTIONS = [ :tag_sets, :ssl ]

    # Get either a URI object or a SRVProtocol URI object.
    #
    # @example Get the uri object.
    #   URI.get(string)
    #
    # @param [ String ] string The URI to parse.
    # @param [ Hash ] opts The options.
    #
    # @option options [ Logger ] :logger A custom logger to use.
    #
    # @return [URI, URI::SRVProtocol] The uri object.
    #
    # @since 2.5.0
    def self.get(string, opts = {})
      unless string
        raise Error::InvalidURI.new(string, 'URI must be a string, not nil.')
      end
      if string.empty?
        raise Error::InvalidURI.new(string, 'Cannot parse an empty URI.')
      end

      scheme, _, _ = string.partition(SCHEME_DELIM)
      case scheme
        when MONGODB_SCHEME
          URI.new(string, opts)
        when MONGODB_SRV_SCHEME
          SRVProtocol.new(string, opts)
        else
          raise Error::InvalidURI.new(string, "Invalid scheme '#{scheme}'. Scheme must be '#{MONGODB_SCHEME}' or '#{MONGODB_SRV_SCHEME}'")
      end
    end

    # Gets the options hash that needs to be passed to a Mongo::Client on
    # instantiation, so we don't have to merge the credentials and database in
    # at that point - we only have a single point here.
    #
    # @example Get the client options.
    #   uri.client_options
    #
    # @return [ Mongo::Options::Redacted ] The options passed to the Mongo::Client
    #
    # @since 2.0.0
    def client_options
      opts = uri_options.tap do |opts|
        opts[:database] = @database if @database
      end

      @user ? opts.merge(credentials) : opts
    end

    def srv_records
      nil
    end

    # Create the new uri from the provided string.
    #
    # @example Create the new URI.
    #   URI.new('mongodb://localhost:27017')
    #
    # @param [ String ] string The URI to parse.
    # @param [ Hash ] options The options.
    #
    # @option options [ Logger ] :logger A custom logger to use.
    #
    # @raise [ Error::InvalidURI ] If the uri does not match the spec.
    #
    # @since 2.0.0
    def initialize(string, options = {})
      unless string
        raise Error::InvalidURI.new(string, 'URI must be a string, not nil.')
      end
      if string.empty?
        raise Error::InvalidURI.new(string, 'Cannot parse an empty URI.')
      end

      @string = string
      @options = options
      parsed_scheme, _, remaining = string.partition(SCHEME_DELIM)
      unless parsed_scheme == scheme
        raise_invalid_error!("Invalid scheme '#{parsed_scheme}'. Scheme must be '#{MONGODB_SCHEME}'. Use URI#get to parse SRV URIs.")
      end
      if remaining.empty?
        raise_invalid_error!('No hosts in the URI')
      end
      parse!(remaining)
      validate_uri_options!
    end

    # Get the credentials provided in the URI.
    #
    # @example Get the credentials.
    #   uri.credentials
    #
    # @return [ Hash ] The credentials.
    #   * :user [ String ] The user.
    #   * :password [ String ] The provided password.
    #
    # @since 2.0.0
    def credentials
      { :user => @user, :password => @password }
    end

    # Get the database provided in the URI.
    #
    # @example Get the database.
    #   uri.database
    #
    # @return [String] The database.
    #
    # @since 2.0.0
    def database
      @database ? @database : Database::ADMIN
    end

    # Get the uri as a string.
    #
    # @example Get the uri as a string.
    #   uri.to_s
    #
    # @return [ String ] The uri string.
    def to_s
      reconstruct_uri
    end

    private

    # Reconstruct the URI from its parts. Invalid options are dropped and options
    # are converted to camelCase.
    #
    # @return [ String ] the uri.
    def reconstruct_uri
      servers = @servers.join(',')
      options = options_mapper.ruby_to_string(@uri_options).map do |k, vs|
        unless vs.nil?
          if vs.is_a?(Array)
            vs.map { |v| "#{k}=#{v}" }.join('&')
          else
            "#{k}=#{vs}"
          end
        end
      end.compact.join('&')

      uri = "#{scheme}#{SCHEME_DELIM}"
      uri += @user.to_s if @user
      uri += "#{AUTH_USER_PWD_DELIM}#{@password}" if @password
      uri += "@" if @user || @password
      uri += @query_hostname || servers
      uri += "/" if @database || !options.empty?
      uri += @database.to_s if @database
      uri += "?#{options}" unless options.empty?
      uri
    end

    def scheme
      MONGODB_SCHEME
    end

    def parse!(remaining)
      hosts_and_db, options = remaining.split('?', 2)
      if options && options.index('?')
        raise_invalid_error!("Options contain an unescaped question mark (?), or the database name contains a question mark and was not escaped")
      end

      if options && !hosts_and_db.index('/')
        raise_invalid_error!("MongoDB URI must have a slash (/) after the hosts if options are given")
      end

      hosts, db = hosts_and_db.split('/', 2)
      if db && db.index('/')
        raise_invalid_error!("Database name contains an unescaped slash (/): #{db}")
      end

      if hosts.index('@')
        creds, hosts = hosts.split('@', 2)
        if hosts.empty?
          raise_invalid_error!("Empty hosts list")
        end
        if hosts.index('@')
          raise_invalid_error!("Unescaped @ in auth info")
        end
      end

      unless hosts.length > 0
        raise_invalid_error!("Missing host; at least one must be provided")
      end

      @servers = hosts.split(',').map do |host|
        if host.empty?
          raise_invalid_error!('Empty host given in the host list')
        end
        decode(host).tap do |host|
          validate_address_str!(host)
        end
      end

      @user = parse_user!(creds)
      @password = parse_password!(creds)
      @uri_options = Options::Redacted.new(parse_uri_options!(options))
      if db
        @database = parse_database!(db)
      end
    rescue Error::InvalidAddress => e
      raise_invalid_error!(e.message)
    end

    def options_mapper
      @options_mapper ||= OptionsMapper.new(
        logger: @options[:logger],
      )
    end

    def parse_uri_options!(string)
      uri_options = {}
      unless string
        return uri_options
      end
      string.split('&').each do |option_str|
        if option_str.empty?
          next
        end
        key, value = option_str.split('=', 2)
        if value.nil?
          raise_invalid_error!("Option #{key} has no value")
        end
        key = decode(key)
        value = decode(value)
        options_mapper.add_uri_option(key, value, uri_options)
      end
      uri_options
    end

    def parse_user!(string)
      if (string && user = string.partition(AUTH_USER_PWD_DELIM)[0])
        raise_invalid_error!(UNESCAPED_USER_PWD) if user =~ UNSAFE
        user_decoded = decode(user)
        if user_decoded =~ PERCENT_CHAR && encode(user_decoded) != user
          raise_invalid_error!(UNESCAPED_USER_PWD)
        end
        user_decoded
      end
    end

    def parse_password!(string)
      if (string && pwd = string.partition(AUTH_USER_PWD_DELIM)[2])
        if pwd.length > 0
          raise_invalid_error!(UNESCAPED_USER_PWD) if pwd =~ UNSAFE
          pwd_decoded = decode(pwd)
          if pwd_decoded =~ PERCENT_CHAR && encode(pwd_decoded) != pwd
            raise_invalid_error!(UNESCAPED_USER_PWD)
          end
          pwd_decoded
        end
      end
    end

    def parse_database!(string)
      raise_invalid_error!(UNESCAPED_DATABASE) if string =~ UNSAFE
      decode(string) if string.length > 0
    end

    def raise_invalid_error!(details)
      raise Error::InvalidURI.new(@string, details, FORMAT)
    end

    def raise_invalid_error_no_fmt!(details)
      raise Error::InvalidURI.new(@string, details)
    end

    def decode(value)
      ::URI::DEFAULT_PARSER.unescape(value)
    end

    def encode(value)
      CGI.escape(value).gsub('+', '%20')
    end

    def validate_uri_options!
      # The URI options spec requires that we raise an error if there are conflicting values of
      # 'tls' and 'ssl'. In order to fulfill this, we parse the values of each instance into an
      # array; assuming all values in the array are the same, we replace the array with that value.
      unless uri_options[:ssl].nil? || uri_options[:ssl].empty?
        unless uri_options[:ssl].uniq.length == 1
          raise_invalid_error_no_fmt!("all instances of 'tls' and 'ssl' must have the same value")
        end

        uri_options[:ssl] = uri_options[:ssl].first
      end

      # Check for conflicting TLS insecure options.
      unless uri_options[:ssl_verify].nil?
        unless uri_options[:ssl_verify_certificate].nil?
          raise_invalid_error_no_fmt!("'tlsInsecure' and 'tlsAllowInvalidCertificates' cannot both be specified")
        end

        unless uri_options[:ssl_verify_hostname].nil?
          raise_invalid_error_no_fmt!("tlsInsecure' and 'tlsAllowInvalidHostnames' cannot both be specified")
        end

        unless uri_options[:ssl_verify_ocsp_endpoint].nil?
          raise_invalid_error_no_fmt!("tlsInsecure' and 'tlsDisableOCSPEndpointCheck' cannot both be specified")
        end
      end

      unless uri_options[:ssl_verify_certificate].nil?
        unless uri_options[:ssl_verify_ocsp_endpoint].nil?
          raise_invalid_error_no_fmt!("tlsAllowInvalidCertificates' and 'tlsDisableOCSPEndpointCheck' cannot both be specified")
        end
      end

      # Since we know that the only URI option that sets :ssl_cert is
      # "tlsCertificateKeyFile", any value set for :ssl_cert must also be set
      # for :ssl_key.
      if uri_options[:ssl_cert]
        uri_options[:ssl_key] = uri_options[:ssl_cert]
      end

      if uri_options[:write_concern] && !uri_options[:write_concern].empty?
        begin
          WriteConcern.get(uri_options[:write_concern])
        rescue Error::InvalidWriteConcern => e
          raise_invalid_error_no_fmt!("#{e.class}: #{e}")
        end
      end

      if uri_options[:direct_connection]
        if uri_options[:connect] && uri_options[:connect].to_s != 'direct'
          raise_invalid_error_no_fmt!("directConnection=true cannot be used with connect=#{uri_options[:connect]}")
        end
        if servers.length > 1
          raise_invalid_error_no_fmt!("directConnection=true cannot be used with multiple seeds")
        end
      elsif uri_options[:direct_connection] == false && uri_options[:connect].to_s == 'direct'
        raise_invalid_error_no_fmt!("directConnection=false cannot be used with connect=direct")
      end

      if uri_options[:load_balanced]
        if servers.length > 1
          raise_invalid_error_no_fmt!("loadBalanced=true cannot be used with multiple seeds")
        end

        if uri_options[:direct_connection]
          raise_invalid_error_no_fmt!("directConnection=true cannot be used with loadBalanced=true")
        end

        if uri_options[:connect] && uri_options[:connect].to_sym == :direct
          raise_invalid_error_no_fmt!("connect=direct cannot be used with loadBalanced=true")
        end

        if uri_options[:replica_set]
          raise_invalid_error_no_fmt!("loadBalanced=true cannot be used with replicaSet option")
        end
      end

      unless self.is_a?(URI::SRVProtocol)
        if uri_options[:srv_max_hosts]
          raise_invalid_error_no_fmt!("srvMaxHosts cannot be used on non-SRV URI")
        end

        if uri_options[:srv_service_name]
          raise_invalid_error_no_fmt!("srvServiceName cannot be used on non-SRV URI")
        end
      end

      if uri_options[:srv_max_hosts] && uri_options[:srv_max_hosts] > 0
        if uri_options[:replica_set]
          raise_invalid_error_no_fmt!("srvMaxHosts > 0 cannot be used with replicaSet option")
        end

        if options[:load_balanced]
          raise_invalid_error_no_fmt!("srvMaxHosts > 0 cannot be used with loadBalanced=true")
        end
      end
    end
  end
end

require 'mongo/uri/options_mapper'
require 'mongo/uri/srv_protocol'
