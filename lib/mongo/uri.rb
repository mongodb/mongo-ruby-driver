# Copyright (C) 2009-2014 MongoDB, Inc.
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
  class URI

    # Create the new uri from the provided string.
    #
    # @example Create the new URI.
    #   URI.new('mongodb://localhost:27017')
    #
    # @param string [String] The uri string.
    # @raise [BadURI] If the uri does not match the spec.
    def initialize(string)
      @match = string.match(URI)
      raise BadURI.new(string) unless @match
    end

    # Get the servers provided in the URI.
    #
    # @example Get the servers.
    #   uri.servers
    #
    # @return [Array<String>] The servers.
    def servers
      @match[3].split(',')
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
      opts = options.merge(:database => database)
      user ? opts.merge(credentials) : opts
    end

    # Get the credentials provided in the URI.
    #
    # @example Get the credentials.
    #   uri.credentials
    #
    # @return [Hash] The credentials.
    #   * :user [String] The user.
    #   * :password [String] The provided password.
    def credentials
      { :user => user, :password => password }
    end

    # Get the database provided in the URI.
    #
    # @example Get the database.
    #   uri.database
    #
    # @return [String] The database.
    def database
      @match[4].empty? ? Database::ADMIN : @match[4]
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
    #   * :tags [Array<Hash>] read tag sets
    def options
      parsed_options = @match[5]
      return {} unless parsed_options
      parsed_options.split('&').reduce({}) do |options, option|
        key, value = option.split('=')
        strategy = OPTION_MAP[key]
        add_option(strategy, value, options)
        options
      end
    end

    # Exception that is raised when trying to parse a URI that does not match
    # the specification.
    class BadURI < RuntimeError

      # Creates a new instance of the BadURI error.
      #
      # @param uri [String] The bad URI.
      def initialize(uri)
        super(message(uri))
      end

      private

      # MongoDB URI format specification
      FORMAT = 'mongodb://[username:password@]host1[:port1][,host2[:port2]' +
        ',...[,hostN[:portN]]][/[database][?options]]'

      # MongoDB URI (connection string) documentation url
      URL = 'http://docs.mongodb.org/manual/reference/connection-string/'

      # Creates a BadURI message
      #
      # @param uri [String] The bad uri.
      # @return [String] The bad uri message.
      def message(uri)
        "MongoDB URI must be in the following format: #{FORMAT}\n" +
        "Please see the following URL for more information: #{URL}\n" +
        "Bad URI: #{uri}"
      end
    end

    private

    # Scheme Regex: non-capturing, matches scheme
    SCHEME = %r{(?:mongodb://)}

    # User Regex: capturing, group 1, matches anything but ':'
    USER = /([^:]+)/

    # Password Regex: capturing, group 2, matches anything but '@'
    PASSWORD = /([^@]+)/

    # Credentials Regex: non capturing, matches 'user:password@'
    CREDENTIALS = /(?:#{USER}:#{PASSWORD}?@)?/

    # Host and port server Regex: matches anything but a forward slash
    HOSTPORT = /[^\/]+/

    # Unix socket server Regex: matches unix socket server
    UNIX = /\/.+.sock?/

    # server Regex: capturing, matches host and port server or unix server
    SERVERS = /((?:(?:#{HOSTPORT}|#{UNIX}),?)+)/

    # Database Regex: matches anything but the characters that cannot
    # be part of any MongoDB database name.
    DATABASE = %r{(?:/([^/\.\ "*<>:\|\?]*))?}

    # Option Regex: notably only matches the ampersand separator and does
    # not allow for semicolon to be used to separate options.
    OPTIONS = /(?:\?(?:(.+=.+)&?)+)*/

    # Complete URI Regex: matches all of the combined components
    URI = /#{SCHEME}#{CREDENTIALS}#{SERVERS}#{DATABASE}#{OPTIONS}/

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
    option 'replicaSet', :replica_set, :type => :replica_set

    # Timeout Options
    option 'connectTimeoutMS', :connect_timeout
    option 'socketTimeoutMS', :socket_timeout

    # Write Options
    option 'w', :w, :group => :write
    option 'j', :j, :group => :write
    option 'fsync', :fsync, :group => :write
    option 'wtimeoutMS', :timeout, :group => :write

    # Read Options
    option 'readPreference', :mode, :group => :read, :type => :read_mode
    option 'readPreferenceTags', :tags, :group => :read, :type => :read_tags

    # Pool options
    option 'minPoolSize', :min_pool_size
    option 'maxPoolSize', :max_pool_size
    option 'waitQueueTimeoutMS', :wait_queue_timeout, :type => :ms_convert

    # Security Options
    option 'ssl', :ssl

    # Auth Options
    option 'authSource', :source, :group => :auth, :type => :auth_source
    option 'authMechanism', :mechanism, :group => :auth, :type => :auth_mech
    option 'authMechanismProperties', :auth_mech_properties, :group => :auth,
           :type => :auth_mech_props

    # Map of URI read preference modes to ruby driver read preference modes
    READ_MODE_MAP = {
      'primary'            => :primary,
      'primaryPreferred'   => :primary_preferred,
      'secondary'          => :secondary,
      'secondaryPreferred' => :secondary_preferred,
      'nearest'            => :nearest
    }.freeze

    # Map of URI authentication mechanisms to ruby driver mechanisms
    AUTH_MECH_MAP = {
      'PLAIN'      => :plain,
      'MONGODB-CR' => :mongodb_cr,
      'GSSAPI'     => :gssapi
    }.freeze

    # Gets the user provided in the URI
    #
    # @return [String] The user.
    def user
      @match[1]
    end

    # Gets the password provided in the URI
    #
    # @return [String] The password.
    def password
      @match[2]
    end

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
