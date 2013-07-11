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

    option 'replicaSet', :replica_set, :transform => :replica_set
    option 'connectTimeoutMS', :connect_timeout
    option 'socketTimeoutMS', :socket_timeout
    option 'w', :w, :type => :write
    option 'j', :j, :type => :write
    option 'fsync', :fsync, :type => :write
    option 'wtimeoutMS', :timeout, :type => :write
    option 'safe', :w, :type => :write, :transform => :safe
    option 'readPreference', :mode, :type => :read, :transform => :read_mode
    option 'readPreferenceTags', :tags, :type => :read, :transform => :read_tags
    option 'slaveOk', :mode, :type => :read, :transform => :slave_ok
    option 'ssl', :ssl
    option 'authSource', :source, :type => :auth, :transform => :auth_source
    option 'authMechanism', :mechanism, :type => :auth, :transform => :auth_mech

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

    def apply_transform(value, transform = nil)
      if transform
        send(transform, value)
      else
        cast(value)
      end
    end

    def select_target(options, type = nil)
      type ? options[type] : options
    end

    def merge_option(target, value, name)
      if target.has_key?(name)
        target[name] += value
      else
        target.merge!({ name => value })
      end
    end

    def add_option(strategy, value, options)
      target = select_target(options, strategy[:type])
      value = apply_transform(value, strategy[:transform])
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
