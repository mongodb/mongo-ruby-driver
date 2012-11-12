require 'rubygems'
require 'test/unit'
require 'tools/mongo_config'

class Test::Unit::TestCase

  TEST_DATA = File.join(File.dirname(__FILE__), 'data')

  def ensure_cluster(kind=nil)
    if defined?(@@current_class) and @@current_class == self.class
      @@cluster.start
    else
      @@current_class = self.class

      if kind == :rs
        opts = Mongo::Config::DEFAULT_REPLICA_SET
        opts.merge!(:arbiters => 2)
      else
        opts = Mongo::Config::DEFAULT_SHARDED_SIMPLE
        opts.merge!(:routers => 4)
      end

      dbpath = ENV['DBPATH'] || 'data'
      opts.merge!(:dbpath => dbpath)

      #debug 1, opts
      config = Mongo::Config.cluster(opts)
      #debug 1, config
      @@cluster = Mongo::Config::ClusterManager.new(config)
      @@cluster.start
    end
    instance_variable_set("@#{kind}", @@cluster)
  end

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure(max_retries=30)
    retries = 0
    begin
      yield
    rescue Mongo::ConnectionFailure => ex
      #puts "Rescue attempt #{retries}: from #{ex}"
      retries += 1
      raise ex if retries > max_retries
      sleep(2)
      retry
    end
  end
end

def silently
  warn_level = $VERBOSE
  $VERBOSE = nil
  begin
    result = yield
  ensure
    $VERBOSE = warn_level
  end
  result
end

begin
  silently { require 'shoulda' }
  silently { require 'mocha' }
rescue LoadError
  puts <<MSG

This test suite requires shoulda and mocha.
You can install them as follows:
  gem install shoulda
  gem install mocha

MSG
  exit
end

require 'bson_ext/cbson' if !(RUBY_PLATFORM =~ /java/) && ENV['C_EXT']

unless defined? MONGO_TEST_DB
  MONGO_TEST_DB = 'ruby-test-db'
end

unless defined? TEST_PORT
  TEST_PORT = ENV['MONGO_RUBY_DRIVER_PORT'] ? ENV['MONGO_RUBY_DRIVER_PORT'].to_i : Mongo::Client::DEFAULT_PORT
end

unless defined? TEST_HOST
  TEST_HOST = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
end

class Test::Unit::TestCase
  include Mongo
  include BSON

  def self.standard_connection(options={})
    Client.new(TEST_HOST, TEST_PORT, options)
  end

  def standard_connection(options={})
    self.class.standard_connection(options)
  end

  def self.host_port
    "#{mongo_host}:#{mongo_port}"
  end

  def self.mongo_host
    TEST_HOST
  end

  def self.mongo_port
    TEST_PORT
  end

  def host_port
    self.class.host_port
  end

  def mongo_host
    self.class.mongo_host
  end

  def mongo_port
    self.class.mongo_port
  end

  def new_mock_socket(host='localhost', port=27017)
    socket = Object.new
    socket.stubs(:setsockopt).with(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    socket.stubs(:close)
    socket.stubs(:closed?)
    socket
  end

  def new_mock_db
    Object.new
  end

  def assert_raise_error(klass, message=nil)
    begin
      yield
    rescue => e
      if klass.to_s != e.class.to_s
        flunk "Expected exception class #{klass} but got #{e.class}.\n #{e.backtrace}"
      end

      if message && !e.message.include?(message)
        p e.backtrace
        flunk "#{e.message} does not include #{message}.\n#{e.backtrace}"
      end
    else
      flunk "Expected assertion #{klass} but none was raised."
    end
  end
end
