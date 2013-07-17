module Mongo
  class URI

    def initialize(string)
      @match = string.match(URI)
      invalid_uri!(string) unless @match
    end

    def nodes
      @match[3].split(',')
    end

    def credentials
      { :user => user, :password => password }
    end

    def database
      @match[4]
    end

    def options
      @match[5].split('&').reduce(Hash.new({})) do |options, option|
        key, value = option.split('=')
        strategy = OPTION_MAP[key]
        add_option(strategy, value, options)
        options
      end
    end

    private

    SCHEME = %r{(?:mongodb://)}
    USER = /([^:]+)/
    PASSWORD = /([^@]+)/
    CREDENTIALS = /(?:#{USER}:#{PASSWORD}?@)?/
    HOSTPORT = /[^\/]+/
    UNIX = /\/.+.sock?/
    NODES = /((?:(?:#{HOSTPORT}|#{UNIX}),?)+)/
    DATABASE = %r{(?:/([^/\.\ "*<>:\|\?]*))?}
    OPTIONS = /(?:\?(?:(.+=.+)&?)+)*/
    URI = /#{SCHEME}#{CREDENTIALS}#{NODES}#{DATABASE}#{OPTIONS}/

    OPTION_MAP = {}

    def self.option(uri_key, name, extra = {})
      OPTION_MAP[uri_key] = { :name => name }.merge(extra)
    end

    # Replica Set
    option 'replicaSet', :replica_set, :type => :replica_set

    # Timeout
    option 'connectTimeoutMS', :connect_timeout
    option 'socketTimeoutMS', :socket_timeout

    # Write
    option 'w', :w, :group => :write
    option 'j', :j, :group => :write
    option 'fsync', :fsync, :group => :write
    option 'wtimeoutMS', :timeout, :group => :write
    option 'safe', :safe, :group => :write

    # Read
    option 'readPreference', :mode, :group => :read, :type => :read_mode
    option 'readPreferenceTags', :tags, :group => :read, :type => :read_tags
    option 'slaveOk', :slave_ok, :group => :read

    # Security
    option 'ssl', :ssl

    # Auth
    option 'authSource', :source, :group => :auth, :type => :auth_source
    option 'authMechanism', :mechanism, :group => :auth, :type => :auth_mech

    READ_MODE_MAP = {
      'primary' => :primary,
      'primaryPreferred' => :primary_preferred,
      'secondary' => :secondary,
      'secondaryPreferred' => :secondary_preferred,
      'nearest' => :nearest
    }.freeze

    AUTH_MECH_MAP = {
      'PLAIN' => :plain,
      'MONGODB-CR' => :mongodb_cr,
      'GSSAPI' => :gssapi
    }.freeze

    def user
      @match[1]
    end

    def password
      @match[2]
    end

    def invalid_uri!(string)
      raise "Bad URI #{string}"
    end

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

    def apply_transform(value, type = nil)
      if type
        send(type, value)
      else
        cast(value)
      end
    end

    def select_target(options, group = nil)
      group ? options[group] : options
    end

    def merge_option(target, value, name)
      if target.has_key?(name)
        target[name] += value
      else
        target.merge!({ name => value })
      end
    end

    def add_option(strategy, value, options)
      target = select_target(options, strategy[:group])
      value = apply_transform(value, strategy[:type])
      merge_option(target, value, strategy[:name])
    end

    def safe(value)
      value == 'true' ? 1 : 0
    end

    def slave_ok(value)
      value == 'true' ? :secondary_preferred : :primary
    end

    def replica_set(value)
      value
    end

    def auth_source(value)
      value == '$external' ? :external : value
    end

    def auth_mech(value)
      AUTH_MECH_MAP[value]
    end

    def read_mode(value)
      READ_MODE_MAP[value]
    end

    def read_tags(value)
      [read_set(value)]
    end

    def read_set(value)
      value.split(',').reduce({}) do |set, tag|
        k, v = tag.split(':')
        set.merge({ k.to_sym => v })
      end
    end
  end
end
