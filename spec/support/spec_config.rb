require 'singleton'

class SpecConfig
  include Singleton

  def initialize
    if ENV['MONGODB_URI']
      @mongodb_uri = Mongo::URI.new(ENV['MONGODB_URI'])
      @uri_options = Mongo::Options::Mapper.transform_keys_to_symbols(@mongodb_uri.uri_options)
      if @uri_options[:replica_set]
        @addresses = @mongodb_uri.servers
        @connect = { connect: :replica_set, replica_set: @uri_options[:replica_set] }
      elsif ENV['TOPOLOGY'] == 'sharded_cluster'
        @addresses = [ @mongodb_uri.servers.first ] # See SERVER-16836 for why we can only use one host:port
        @connect = { connect: :sharded }
      else
        @addresses = @mongodb_uri.servers
        @connect = { connect: :direct }
      end
      if @uri_options[:ssl].nil?
        @ssl = (ENV['SSL'] == 'ssl') || (ENV['SSL_ENABLED'] == 'true')
      else
        @ssl = @uri_options[:ssl]
      end
    else
      @addresses = ENV['MONGODB_ADDRESSES'] ? ENV['MONGODB_ADDRESSES'].split(',').freeze : [ '127.0.0.1:27017' ].freeze
      if ENV['RS_ENABLED']
        @connect = { connect: :replica_set, replica_set: ENV['RS_NAME'] }
      elsif ENV['SHARDED_ENABLED']
        @connect = { connect: :sharded }
      else
        @connect = { connect: :direct }
      end
    end
  end

  def mri?
    !jruby?
  end

  def jruby?
    RUBY_PLATFORM =~ /\bjava\b/
  end

  def platform
    RUBY_PLATFORM
  end

  def client_debug?
    %w(1 true yes).include?((ENV['CLIENT_DEBUG'] || '').downcase)
  end

  attr_reader :uri_options, :addresses, :connect

  def user
    @mongodb_uri && @mongodb_uri.credentials[:user]
  end

  def password
    @mongodb_uri && @mongodb_uri.credentials[:password]
  end

  def auth_source
    @uri_options && uri_options[:auth_source]
  end

  def connect_replica_set?
    connect[:connect] == :replica_set
  end

  def any_port
    addresses.first.split(':')[1] || '27017'
  end

  def ssl?
    @ssl
  end

  def ci?
    !!ENV['CI']
  end
end
