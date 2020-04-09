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
  # http://docs.mongodb.org/manual/reference/connection-string/
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
    HELP = 'http://docs.mongodb.org/manual/reference/connection-string/'.freeze

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

      scheme, _, remaining = string.partition(SCHEME_DELIM)
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

    private

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

    def extract_db_opts!(string)
      db_opts, _, creds_hosts = string.reverse.partition(DATABASE_DELIM)
      db_opts, creds_hosts = creds_hosts, db_opts if creds_hosts.empty?
      if db_opts.empty? && creds_hosts.include?(URI_OPTS_DELIM)
        raise_invalid_error!(INVALID_OPTS_DELIM)
      end
      [ creds_hosts, db_opts ].map { |s| s.reverse }
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
        add_uri_option(key, value, uri_options)
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
    uri_option 'replicaset', :replica_set

    # Timeout Options
    uri_option 'connecttimeoutms', :connect_timeout, :type => :ms
    uri_option 'sockettimeoutms', :socket_timeout, :type => :ms
    uri_option 'serverselectiontimeoutms', :server_selection_timeout, :type => :ms
    uri_option 'localthresholdms', :local_threshold, :type => :ms
    uri_option 'heartbeatfrequencyms', :heartbeat_frequency, :type => :ms
    uri_option 'maxidletimems', :max_idle_time, :type => :ms

    # Write Options
    uri_option 'w', :w, :group => :write_concern, type: :w
    uri_option 'journal', :j, :group => :write_concern, :type => :bool
    uri_option 'fsync', :fsync, :group => :write_concern, type: :bool
    uri_option 'wtimeoutms', :wtimeout, :group => :write_concern, :type => :integer

    # Read Options
    uri_option 'readpreference', :mode, :group => :read, :type => :read_mode
    uri_option 'readpreferencetags', :tag_sets, :group => :read, :type => :read_tags
    uri_option 'maxstalenessseconds', :max_staleness, :group => :read, :type => :max_staleness

    # Pool options
    uri_option 'minpoolsize', :min_pool_size, :type => :integer
    uri_option 'maxpoolsize', :max_pool_size, :type => :integer
    uri_option 'waitqueuetimeoutms', :wait_queue_timeout, :type => :ms

    # Security Options
    uri_option 'ssl', :ssl, :type => :repeated_bool
    uri_option 'tls', :ssl, :type => :repeated_bool
    uri_option 'tlsallowinvalidcertificates', :ssl_verify_certificate,
               :type => :inverse_bool
    uri_option 'tlsallowinvalidhostnames', :ssl_verify_hostname,
               :type => :inverse_bool
    uri_option 'tlscafile', :ssl_ca_cert
    uri_option 'tlscertificatekeyfile', :ssl_cert
    uri_option 'tlscertificatekeyfilepassword', :ssl_key_pass_phrase
    uri_option 'tlsinsecure', :ssl_verify, :type => :inverse_bool

    # Topology options
    uri_option 'directconnection', :direct_connection, type: :bool
    uri_option 'connect', :connect, type: :symbol

    # Auth Options
    uri_option 'authsource', :auth_source
    uri_option 'authmechanism', :auth_mech, :type => :auth_mech
    uri_option 'authmechanismproperties', :auth_mech_properties, :type => :auth_mech_props

    # Client Options
    uri_option 'appname', :app_name
    uri_option 'compressors', :compressors, :type => :array
    uri_option 'readconcernlevel', :level, group: :read_concern, type: :symbol
    uri_option 'retryreads', :retry_reads, :type => :bool
    uri_option 'retrywrites', :retry_writes, :type => :bool
    uri_option 'zlibcompressionlevel', :zlib_compression_level, :type => :zlib_compression_level

    # Applies URI value transformation by either using the default cast
    # or a transformation appropriate for the given type.
    #
    # @param key [String] URI option name.
    # @param value [String] The value to be transformed.
    # @param type [Symbol] The transform method.
    def apply_transform(key, value, type)
      if type
        if respond_to?("convert_#{type}", true)
          send("convert_#{type}", key, value)
        else
          send(type, value)
        end
      else
        value
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
    # @param key [String] URI option name.
    # @param value [String] The value of the option.
    # @param uri_options [Hash] The base option target.
    def add_uri_option(key, value, uri_options)
      strategy = URI_OPTION_MAP[key.downcase]
      if strategy.nil?
        log_warn("Unsupported URI option '#{key}' on URI '#{@string}'. It will be ignored.")
        return
      end

      target = select_target(uri_options, strategy[:group])
      value = apply_transform(key, value, strategy[:type])
      merge_uri_option(target, value, strategy[:name])
    end

    # Authentication mechanism transformation.
    #
    # @param value [String] The authentication mechanism.
    #
    # @return [Symbol] The transformed authentication mechanism.
    def auth_mech(value)
      (AUTH_MECH_MAP[value.upcase] || value).tap do |mech|
        log_warn("#{value} is not a valid auth mechanism") unless mech
      end
    end

    # Read preference mode transformation.
    #
    # @param value [String] The read mode string value.
    #
    # @return [Symbol] The read mode symbol.
    def read_mode(value)
      READ_MODE_MAP[value.downcase] || value
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
      hash_extractor('readPreferenceTags', value)
    end

    # Auth mechanism properties extractor.
    #
    # @param value [ String ] The auth mechanism properties string.
    #
    # @return [ Hash ] The auth mechanism properties hash.
    def auth_mech_props(value)
      properties = hash_extractor('authMechanismProperties', value)
      if properties && properties[:canonicalize_host_name]
        properties.merge!(canonicalize_host_name:
          properties[:canonicalize_host_name].downcase == 'true')
      end
      properties
    end

    # Parses the zlib compression level.
    #
    # @param value [ String ] The zlib compression level string.
    #
    # @return [ Integer | nil ] The compression level value if it is between -1 and 9 (inclusive),
    #   otherwise nil (and a warning will be logged).
    def zlib_compression_level(value)
      if /\A-?\d+\z/ =~ value
        i = value.to_i

        if i >= -1 && i <= 9
          return i
        end
      end

      log_warn("#{value} is not a valid zlibCompressionLevel")
      nil
    end

    # Converts the value into a boolean and returns it wrapped in an array.
    #
    # @param name [ String ] Name of the URI option being processed.
    # @param value [ String ] URI option value.
    #
    # @return [ Array<true | false> ] The boolean value parsed and wraped
    #   in an array.
    def convert_repeated_bool(name, value)
      [convert_bool(name, value)]
    end

    # Converts +value+ into an integer.
    #
    # If the value is not a valid integer, warns and returns nil.
    #
    # @param name [ String ] Name of the URI option being processed.
    # @param value [ String ] URI option value.
    #
    # @return [ nil | Integer ] Converted value.
    def convert_integer(name, value)
      unless /\A\d+\z/ =~ value
        log_warn("#{value} is not a valid integer for #{name}")
        return nil
      end

      value.to_i
    end

    # Converts +value+ into a symbol.
    #
    # @param name [ String ] Name of the URI option being processed.
    # @param value [ String ] URI option value.
    #
    # @return [ Symbol ] Converted value.
    def convert_symbol(name, value)
      value.to_sym
    end

    # Converts +value+ as a write concern.
    #
    # If +value+ is the word "majority", returns the symbol :majority.
    # If +value+ is a number, returns the number as an integer.
    # Otherwise returns the string +value+ unchanged.
    #
    # @param name [ String ] Name of the URI option being processed.
    # @param value [ String ] URI option value.
    #
    # @return [ Integer | Symbol | String ] Converted value.
    def convert_w(name, value)
      case value
      when 'majority'
        :majority
      when /\A[0-9]+\z/
        value.to_i
      else
        value
      end
    end

    # Converts +value+ to a boolean.
    #
    # Returns true for 'true', false for 'false', otherwise nil.
    #
    # @param name [ String ] Name of the URI option being processed.
    # @param value [ String ] URI option value.
    #
    # @return [ true | false | nil ] Converted value.
    def convert_bool(name, value)
      case value
      when "true", 'TRUE'
        true
      when "false", 'FALSE'
        false
      else
        log_warn("invalid boolean option for #{name}: #{value}")
        nil
      end
    end

    # Parses a boolean value and returns its inverse.
    #
    # @param value [ String ] The URI option value.
    #
    # @return [ true | false | nil ] The inverse of the  boolean value parsed out, otherwise nil
    #   (and a warning will be logged).
    def convert_inverse_bool(name, value)
      b = convert_bool(name, value)

      if b.nil?
        nil
      else
        !b
      end
    end

    # Parses the max staleness value, which must be either "0" or an integer greater or equal to 90.
    #
    # @param value [ String ] The max staleness string.
    #
    # @return [ Integer | nil ] The max staleness integer parsed out if it is valid, otherwise nil
    #   (and a warning will be logged).
    def max_staleness(value)
      if /\A-?\d+\z/ =~ value
        int = value.to_i

        if int == -1
          int = nil
        end

        if int && (int >= 0 && int < 90 || int < 0)
          log_warn("max staleness should be either 0 or greater than 90: #{value}")
        end

        return int
      end

      log_warn("Invalid max staleness value: #{value}")
      nil
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
    def convert_ms(name, value)
      unless /\A-?\d+(\.\d+)?\z/ =~ value
        log_warn("Invalid ms value for #{name}: #{value}")
        return nil
      end

      if value[0] == '-'
        log_warn("#{name} cannot be a negative number")
        return nil
      end

      value.to_f / 1000
    end

    # Extract values from the string and put them into a nested hash.
    #
    # @param value [ String ] The string to build a hash from.
    #
    # @return [ Hash ] The hash built from the string.
    def hash_extractor(name, value)
      h = {}
      value.split(',').each do |tag|
        k, v = tag.split(':')
        if v.nil?
          log_warn("Invalid hash value for #{name}: key `#{k}` does not have a value: #{value}")
        end

        h[k.downcase.to_sym] = v
      end
      h
    end

    # Extract values from the string and put them into an array.
    #
    # @param [ String ] value The string to build an array from.
    #
    # @return [ Array ] The array built from the string.
    def array(value)
      value.split(',')
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
      end

      # Since we know that the only URI option that sets :ssl_cert is "tlsCertificateKeyFile", any
      # value set for :ssl_cert must also be set for :ssl_key.
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
    end
  end
end

require 'mongo/uri/srv_protocol'
