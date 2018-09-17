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

  # The write concern to use in the tests.
  def write_concern
    if connect_replica_set?
      {w: 2}
    else
      {w: 1}
    end
  end

  def any_port
    addresses.first.split(':')[1] || '27017'
  end

  def drivers_tools?
    !!ENV['DRIVERS_TOOLS']
  end

  def ssl?
    @ssl
  end

  def spec_root
    File.join(File.dirname(__FILE__), '..')
  end

  def ssl_certs_dir
    "#{spec_root}/support/certificates"
  end

  def client_cert_pem
    if drivers_tools?
      ENV['DRIVER_TOOLS_CLIENT_CERT_PEM']
    else
      "#{ssl_certs_dir}/client_cert.pem"
    end
  end

  def client_key_pem
    if drivers_tools?
      ENV['DRIVER_TOOLS_CLIENT_KEY_PEM']
    else
      "#{ssl_certs_dir}/client_key.pem"
    end
  end

  def ssl_options
    if ssl?
      {
        ssl: true,
        ssl_verify: false,
        ssl_cert:  client_cert_pem,
        ssl_key:  client_key_pem,
      }
    else
      {}
    end
  end

  # What compressor to use, if any.
  def compressors
    if ENV['COMPRESSORS']
      ENV['COMPRESSORS'].split(',')
    else
      nil
    end
  end

  def compressor_options
    if compressors
      {compressors: compressors}
    else
      {}
    end
  end

  def ci?
    !!ENV['CI']
  end
end
