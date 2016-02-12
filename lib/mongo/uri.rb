# Copyright (C) 2014-2015 MongoDB, Inc.
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

require 'uri'

module Mongo

  # The URI class provides a way for users to parse the MongoDB uri as
  # defined in the connection string format spec.
  #
  # http://docs.mongodb.org/manual/reference/connection-string/
  #
  # @example Use the uri string to make a client connection.
  #   uri = URI.new('mongodb://localhost:27017')
  #   client = Client.new(uri.server, uri.options)
  #   client.login(uri.credentials)
  #   client[uri.database]
  #
  # @since 2.0.0
  class URI
    include Loggable

    # The uri parser object options.
    #
    # @since 2.0.0
    attr_reader :options

    # The options specified in the uri.
    #
    # @since 2.1.0
    attr_reader :uri_options

    # The servers specified in the uri.
    #
    # @since 2.0.0
    attr_reader :servers

    # Unsafe characters that must be urlencoded.
    #
    # @since 2.1.0
    UNSAFE = /[\:\/\+\@]/

    # Unix socket suffix.
    #
    # @since 2.1.0
    UNIX_SOCKET = /.sock/

    # The mongodb connection string scheme.
    #
    # @since 2.0.0
    SCHEME = 'mongodb://'.freeze

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

    # Error details for an invalid scheme.
    #
    # @since 2.1.0
    INVALID_SCHEME = "Invalid scheme. Scheme must be '#{SCHEME}'".freeze

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

    # Error details for a non-urlencoded auth databsae name.
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

    # MongoDB URI format specification.
    #
    # @since 2.0.0
    FORMAT = 'mongodb://[username:password@]host1[:port1][,host2[:port2]' +
      ',...[,hostN[:portN]]][/[database][?options]]'.freeze

    # MongoDB URI (connection string) documentation url
    #
    # @since 2.0.0
    HELP = 'http://docs.mongodb.org/manual/reference/connection-string/'.freeze

    # Map of URI read preference modes to ruby driver read preference modes
    #
    # @since 2.0.0
    READ_MODE_MAP = {
      'primary'            => :primary,
      'primaryPreferred'   => :primary_preferred,
      'secondary'          => :secondary,
      'secondaryPreferred' => :secondary_preferred,
      'nearest'            => :nearest
    }.freeze

    # Map of URI authentication mechanisms to ruby driver mechanisms
    #
    # @since 2.0.0
    AUTH_MECH_MAP = {
      'PLAIN'       => :plain,
      'MONGODB-CR'  => :mongodb_cr,
      'GSSAPI'      => :gssapi,
      'SCRAM-SHA-1' => :scram
    }.freeze

    # Options that are allowed to appear more than once in the uri.
    #
    # @since 2.1.0
    REPEATABLE_OPTIONS = [ :tag_sets ]

    # Create the new uri from the provided string.
    #
    # @example Create the new URI.
    #   URI.new('mongodb://localhost:27017')
    #
    # @param [ String ] string The uri string.
    # @param [ Hash ] options The options.
    #
    # @raise [ Error::InvalidURI ] If the uri does not match the spec.
    #
    # @since 2.0.0
    def initialize(string, options = {})
      @string = string
      @options = options
      empty, scheme, remaining = @string.partition(SCHEME)
      raise_invalid_error!(INVALID_SCHEME) unless scheme == SCHEME
      setup!(remaining)
    end

    # Gets the options hash that needs to be passed to a Mongo::Client on
    # instantiation, so we don't have to merge the credentials and database in
    # at that point - we only have a single point here.
    #
    # @example Get the client options.
    #   uri.client_options
    #
    # @return [ Hash ] The options passed to the Mongo::Client
    #
    # @since 2.0.0
    def client_options
      opts = uri_options.merge(:database => database)
      @user ? opts.merge(credentials) : opts
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

    private

    def setup!(remaining)
      creds_hosts, db_opts = extract_db_opts!(remaining)
      parse_creds_hosts!(creds_hosts)
      parse_db_opts!(db_opts)
    end

    def extract_db_opts!(string)
      db_opts, d, creds_hosts = string.reverse.partition(DATABASE_DELIM)
      db_opts, creds_hosts = creds_hosts, db_opts if creds_hosts.empty?
      if db_opts.empty? && creds_hosts.include?(URI_OPTS_DELIM)
        raise_invalid_error!(INVALID_OPTS_DELIM)
      end
      [ creds_hosts, db_opts ].map { |s| s.reverse }
    end

    def parse_creds_hosts!(string)
      hosts, creds = split_creds_hosts(string)
      @servers = parse_servers!(hosts)
      @user = parse_user!(creds)
      @password = parse_password!(creds)
    end

    def split_creds_hosts(string)
      hosts, d, creds = string.reverse.partition(AUTH_DELIM)
      hosts, creds = creds, hosts if hosts.empty?
      [ hosts, creds ].map { |s| s.reverse }
    end

    def parse_db_opts!(string)
      auth_db, d, uri_opts = string.partition(URI_OPTS_DELIM)
      @uri_options = Options::Redacted.new(parse_uri_options!(uri_opts))
      @database = parse_database!(auth_db)
    end

    def parse_uri_options!(string)
      return {} unless string
      string.split(INDIV_URI_OPTS_DELIM).reduce({}) do |uri_options, opt|
        raise_invalid_error!(INVALID_OPTS_VALUE_DELIM) unless opt.index(URI_OPTS_VALUE_DELIM)
        key, value = opt.split(URI_OPTS_VALUE_DELIM)
        strategy = URI_OPTION_MAP[key.downcase]
        if strategy.nil?
          log_warn("Unsupported URI option '#{key}' on URI '#{@string}'. It will be ignored.")
        else
          add_uri_option(strategy, value, uri_options)
        end
        uri_options
      end
    end

    def parse_user!(string)
      if (string && user = string.partition(AUTH_USER_PWD_DELIM)[0])
        raise_invalid_error!(UNESCAPED_USER_PWD) if user =~ UNSAFE
        decode(user) if user.length > 0
      end
    end

    def parse_password!(string)
      if (string && pwd = string.partition(AUTH_USER_PWD_DELIM)[2])
        raise_invalid_error!(UNESCAPED_USER_PWD) if pwd =~ UNSAFE
        decode(pwd) if pwd.length > 0
      end
    end

    def parse_database!(string)
      raise_invalid_error!(UNESCAPED_DATABASE) if string =~ UNSAFE
      decode(string) if string.length > 0
    end

    def validate_port_string!(port)
      unless port.nil? || (port.length > 0 && port.to_i > 0 && port.to_i <= 65535)
        raise_invalid_error!(INVALID_PORT)
      end
    end

    def parse_servers!(string)
      raise_invalid_error!(INVALID_HOST) unless string.size > 0
      string.split(HOST_DELIM).reduce([]) do |servers, host|
        if host[0] == '['
          if host.index(']:')
            h, p = host.split(']:')
            validate_port_string!(p)
          end
        elsif host.index(HOST_PORT_DELIM)
          h, d, p = host.partition(HOST_PORT_DELIM)
          raise_invalid_error!(INVALID_HOST) unless h.size > 0
          validate_port_string!(p)
        elsif host =~ UNIX_SOCKET
          raise_invalid_error!(UNESCAPED_UNIX_SOCKET) if host =~ UNSAFE
        end
        servers << host
      end
    end

    def raise_invalid_error!(details)
      raise Error::InvalidURI.new(@string, details)
    end

    def decode(value)
      ::URI.decode(value)
    end

    # Hash for storing map of URI option parameters to conversion strategies
    URI_OPTION_MAP = {}

    # Simple internal dsl to register a MongoDB URI option in the URI_OPTION_MAP.
    #
    # @param uri_key [String] The MongoDB URI option to register.
    # @param name [Symbol] The name of the option in the driver.
    # @param extra [Hash] Extra options.
    #   * :group [Symbol] Nested hash where option will go.
    #   * :type [Symbol] Name of function to transform value.
    def self.uri_option(uri_key, name, extra = {})
      URI_OPTION_MAP[uri_key] = { :name => name }.merge(extra)
    end

    # Replica Set Options
    uri_option 'replicaset', :replica_set, :type => :replica_set

    # Timeout Options
    uri_option 'connecttimeoutms', :connect_timeout, :type => :ms_convert
    uri_option 'sockettimeoutms', :socket_timeout, :type => :ms_convert
    uri_option 'serverselectiontimeoutms', :server_selection_timeout, :type => :ms_convert
    uri_option 'localthresholdms', :local_threshold, :type => :ms_convert

    # Write Options
    uri_option 'w', :w, :group => :write
    uri_option 'journal', :j, :group => :write
    uri_option 'fsync', :fsync, :group => :write
    uri_option 'wtimeoutms', :timeout, :group => :write

    # Read Options
    uri_option 'readpreference', :mode, :group => :read, :type => :read_mode
    uri_option 'readpreferencetags', :tag_sets, :group => :read, :type => :read_tags

    # Pool options
    uri_option 'minpoolsize', :min_pool_size
    uri_option 'maxpoolsize', :max_pool_size
    uri_option 'waitqueuetimeoutms', :wait_queue_timeout, :type => :ms_convert

    # Security Options
    uri_option 'ssl', :ssl

    # Topology options
    uri_option 'connect', :connect

    # Auth Options
    uri_option 'authsource', :source, :group => :auth, :type => :auth_source
    uri_option 'authmechanism', :auth_mech, :type => :auth_mech
    uri_option 'authmechanismproperties', :auth_mech_properties, :group => :auth,
                 :type => :auth_mech_props

    # Casts option values that do not have a specifically provided
    # transofrmation to the appropriate type.
    #
    # @param value [String] The value to be cast.
    #
    # @return [true, false, Fixnum, Symbol] The cast value.
    def cast(value)
      if value == 'true'
        true
      elsif value == 'false'
        false
      elsif value =~ /[\d]/
        value.to_i
      else
        decode(value).to_sym
      end
    end

    # Applies URI value transformation by either using the default cast
    # or a transformation appropriate for the given type.
    #
    # @param value [String] The value to be transformed.
    # @param type [Symbol] The transform method.
    def apply_transform(value, type = nil)
      if type
        send(type, value)
      else
        cast(value)
      end
    end

    # Selects the output destination for an option.
    #
    # @param [Hash] uri_options The base target.
    # @param [Symbol] group Group subtarget.
    #
    # @return [Hash] The target for the option.
    def select_target(uri_options, group = nil)
      if group
        uri_options[group] ||= {}
      else
        uri_options
      end
    end

    # Merges a new option into the target.
    #
    # If the option exists at the target destination the merge will
    # be an addition.
    #
    # Specifically required to append an additional tag set
    # to the array of tag sets without overwriting the original.
    #
    # @param target [Hash] The destination.
    # @param value [Object] The value to be merged.
    # @param name [Symbol] The name of the option.
    def merge_uri_option(target, value, name)
      if target.key?(name)
        if REPEATABLE_OPTIONS.include?(name)
          target[name] += value
        else
          log_warn("Repeated option key: #{name}.")
        end
      else
        target.merge!(name => value)
      end
    end

    # Adds an option to the uri options hash via the supplied strategy.
    #
    #   Acquires a target for the option based on group.
    #   Transforms the value.
    #   Merges the option into the target.
    #
    # @param strategy [Symbol] The strategy for this option.
    # @param value [String] The value of the option.
    # @param uri_options [Hash] The base option target.
    def add_uri_option(strategy, value, uri_options)
      target = select_target(uri_options, strategy[:group])
      value = apply_transform(value, strategy[:type])
      merge_uri_option(target, value, strategy[:name])
    end

    # Replica set transformation, avoid converting to Symbol.
    #
    # @param value [String] Replica set name.
    #
    # @return [String] Same value to avoid cast to Symbol.
    def replica_set(value)
      decode(value)
    end

    # Auth source transformation, either db string or :external.
    #
    # @param value [String] Authentication source.
    #
    # @return [String] If auth source is database name.
    # @return [:external] If auth source is external authentication.
    def auth_source(value)
      value == '$external' ? :external : decode(value)
    end

    # Authentication mechanism transformation.
    #
    # @param value [String] The authentication mechanism.
    #
    # @return [Symbol] The transformed authentication mechanism.
    def auth_mech(value)
      AUTH_MECH_MAP[value]
    end

    # Read preference mode transformation.
    #
    # @param value [String] The read mode string value.
    #
    # @return [Symbol] The read mode symbol.
    def read_mode(value)
      READ_MODE_MAP[value]
    end

    # Read preference tags transformation.
    #
    # @param value [String] The string representing tag set.
    #
    # @return [Array<Hash>] Array with tag set.
    def read_tags(value)
      [read_set(value)]
    end

    # Read preference tag set extractor.
    #
    # @param value [String] The tag set string.
    #
    # @return [Hash] The tag set hash.
    def read_set(value)
      hash_extractor(value)
    end

    # Auth mechanism properties extractor.
    #
    # @param value [ String ] The auth mechanism properties string.
    #
    # @return [ Hash ] The auth mechanism properties hash.
    def auth_mech_props(value)
      properties = hash_extractor(value)
      if properties[:canonicalize_host_name]
        properties.merge!(canonicalize_host_name:
                            properties[:canonicalize_host_name] == 'true')
      end
      properties
    end

    # Ruby's convention is to provide timeouts in seconds, not milliseconds and
    # to use fractions where more precision is necessary. The connection string
    # options are always in MS so we provide an easy conversion type.
    #
    # @param [ Integer ] value The millisecond value.
    #
    # @return [ Float ] The seconds value.
    #
    # @since 2.0.0
    def ms_convert(value)
      value.to_f / 1000
    end

    # Extract values from the string and put them into a nested hash.
    #
    # @param value [ String ] The string to build a hash from.
    #
    # @return [ Hash ] The hash built from the string.
    def hash_extractor(value)
      value.split(',').reduce({}) do |set, tag|
        k, v = tag.split(':')
        set.merge(decode(k).downcase.to_sym => decode(v))
      end
    end
  end
end
