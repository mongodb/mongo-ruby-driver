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

    attr_reader :options

    attr_reader :uri_options
    attr_reader :user
    attr_reader :password
    attr_reader :servers

    UNSAFE = /[\:\/\+\@]/

    # The mongodb connection string scheme.
    #
    # @since 2.1.0
    SCHEME = 'mongodb://'.freeze

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
      'PLAIN'      => :plain,
      'MONGODB-CR' => :mongodb_cr,
      'GSSAPI'     => :gssapi
    }.freeze

    # Create the new uri from the provided string.
    #
    # @example Create the new URI.
    #   URI.new('mongodb://localhost:27017')
    #
    # @param [ String ] string The uri string.
    # @param [ Hash ] options The options.
    #
    # @raise [ BadURI ] If the uri does not match the spec.
    #
    # @since 2.0.0
    def initialize(string, options = {})
      @string = string
      @options = options
      @remaining = @string.split(SCHEME)[1]
      raise Error::InvalidURI.new(@string) unless @remaining
      setup!
    end

    # Get the servers provided in the URI.
    #
    # @example Get the servers.
    #   uri.servers
    #
    # @return [ Array<String> ] The servers.
    #
    # @since 2.0.0
    def parse_servers!
      raise Error::InvalidURI.new(@string) unless @remaining.size > 0
      @servers ||= @remaining.split(',').reduce([]) do |servers, host|
        if host[0] == '['
          if host.index(']:')
            h, p = host.split(']:')
          raise Error::InvalidURI.new(@string) unless p.length > 0
          raise Error::InvalidURI.new(@string) unless p.to_i > 0 && p.to_i <= 65535
          end
        elsif host.index(':')
          raise Error::InvalidURI.new(@string) unless host.count(':') == 1
          h, p = host.split(':')
          raise Error::InvalidURI.new(@string) unless h
          raise Error::InvalidURI.new(@string) unless p.length > 0
          raise Error::InvalidURI.new(@string) unless p.to_i > 0 && p.to_i <= 65535
        end
        servers << host
      end
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
      user ? opts.merge(credentials) : opts
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
      { :user => user, :password => password }
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
      @database || Database::ADMIN
    end

    # Get the options provided in the URI.
    #
    # @example Get The options.
    #   uri.options
    #
    # @return [Hash] The options.
    #
    #   Generic Options
    #   * :replica_set [String] replica set name
    #   * :connect_timeout [Fixnum] connect timeout
    #   * :socket_timeout [Fixnum] socket timeout
    #   * :ssl [true, false] ssl enabled?
    #
    #   Write Options (returned in a hash under the :write key)
    #   * :w [String, Fixnum] write concern value
    #   * :j [true, false] journal
    #   * :fsync [true, false] fsync
    #   * :timeout [Fixnum] timeout for write operation
    #
    #   Read Options (returned in a hash under the :read key)
    #   * :mode [Symbol]  read mode
    #   * :tag_sets [Array<Hash>] read tag sets
    #
    # @since 2.0.0


    def parse_options!
      return {} unless @options_part
      @options_part.split('&').reduce({}) do |options, option|
        raise Error::InvalidURI.new(@string) unless option.index('=')
        key, value = option.split('=')
        strategy = OPTION_MAP[key.downcase]
        if strategy.nil?
          log_warn("Unsupported URI option '#{key}' on URI '#{@string}'. It will be ignored.")
        else
          add_option(strategy, value, options)
        end
        options
      end
    end

    private

    def extract_options!
      if @remaining.index('?')
        @options_part = @remaining[@remaining.index('?')+1..-1]
        @remaining = @remaining[0...@remaining.index('?')]
      end
      parse_options!
    end

    def parse_user!
      if (@auth_part && user = @auth_part.partition(':')[0])
        escaped = ::URI.encode(user)
        raise Error::InvalidURI.new(@string) if user =~ UNSAFE
        user
      end
    end

    def parse_password!
      if (@auth_part && pwd = @auth_part.partition(':')[2])
        raise Error::InvalidURI.new(@string) if pwd =~ UNSAFE
        pwd
      end
    end

    def extract_auth!
      if @remaining.index('@')
        @auth_part = @remaining[0...-(@remaining.reverse.index('@')+1)]
        @remaining = @remaining[@auth_part.size+1..-1]
      end
      [ parse_user!, parse_password! ]
    end

    def extract_database!
      if @remaining.index('/')
        if @remaining.reverse.index('/') == 0
            @database_part = nil
            @remaining = @remaining[0...-1]
        else
          db = @remaining[-(@remaining.reverse.index('/'))..-1]
          unless db.end_with?('.sock')
            @database_part = db
            @remaining = @remaining[0..-(@database_part.size+2)]
          end
        end
      elsif @options_part
        raise Exception 'Need a slash before options'
      end
      @database_part
    end

    def extract_servers!
      parse_servers!
    end

    def setup!
      @uri_options = extract_options!
      @user, @password = extract_auth!
      @database = extract_database!
      @servers = extract_servers!
    end

    # Hash for storing map of URI option parameters to conversion strategies
    OPTION_MAP = {}

    # Simple internal dsl to register a MongoDB URI option in the OPTION_MAP.
    #
    # @param uri_key [String] The MongoDB URI option to register.
    # @param name [Symbol] The name of the option in the driver.
    # @param extra [Hash] Extra options.
    #   * :group [Symbol] Nested hash where option will go.
    #   * :type [Symbol] Name of function to transform value.
    def self.option(uri_key, name, extra = {})
      OPTION_MAP[uri_key] = { :name => name }.merge(extra)
    end

    # Replica Set Options
    option 'replicaset', :replica_set, :type => :replica_set

    # Timeout Options
    option 'connecttimeoutms', :connect_timeout, :type => :ms_convert
    option 'sockettimeoutms', :socket_timeout, :type => :ms_convert
    option 'serverselectiontimeoutms', :server_selection_timeout, :type => :ms_convert
    option 'localthresholdms', :local_threshold, :type => :ms_convert

    # Write Options
    option 'w', :w, :group => :write
    option 'journal', :j, :group => :write
    option 'fsync', :fsync, :group => :write
    option 'wtimeoutms', :timeout, :group => :write

    # Read Options
    option 'readpreference', :mode, :group => :read, :type => :read_mode
    option 'readpreferencetags', :tag_sets, :group => :read, :type => :read_tags

    # Pool options
    option 'minpoolsize', :min_pool_size
    option 'maxpoolsize', :max_pool_size
    option 'waitqueuetimeoutms', :wait_queue_timeout, :type => :ms_convert

    # Security Options
    option 'ssl', :ssl

    # Topology options
    option 'connect', :connect

    # Auth Options
    option 'authsource', :source, :group => :auth, :type => :auth_source
    option 'authmechanism', :auth_mech, :type => :auth_mech
    option 'authmechanismproperties', :auth_mech_properties, :group => :auth,
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
        value.to_sym
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
    # @param options [Hash] The base target.
    # @param group [Symbol] Group subtarget.
    #
    # @return [Hash] The target for the option.
    def select_target(options, group = nil)
      if group
        options[group] ||= {}
      else
        options
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
    def merge_option(target, value, name)
      if target.key?(name)
        target[name] += value
      else
        target.merge!(name => value)
      end
    end

    # Adds an option to the options hash via the supplied strategy.
    #
    #   Acquires a target for the option based on group.
    #   Transforms the value.
    #   Merges the option into the target.
    #
    # @param strategy [Symbol] The strategy for this option.
    # @param value [String] The value of the option.
    # @param options [Hash] The base option target.
    def add_option(strategy, value, options)
      target = select_target(options, strategy[:group])
      value = apply_transform(value, strategy[:type])
      merge_option(target, value, strategy[:name])
    end

    # Replica set transformation, avoid converting to Symbol.
    #
    # @param value [String] Replica set name.
    #
    # @return [String] Same value to avoid cast to Symbol.
    def replica_set(value)
      value
    end

    # Auth source transformation, either db string or :external.
    #
    # @param value [String] Authentication source.
    #
    # @return [String] If auth source is database name.
    # @return [:external] If auth source is external authentication.
    def auth_source(value)
      value == '$external' ? :external : value
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
        set.merge(k.downcase.to_sym => v)
      end
    end
  end
end
