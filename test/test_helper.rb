require 'rubygems'
# SimpleCov must load before our code - A coverage report summary line will print after each test suite
if RUBY_VERSION >= '1.9.0' && RUBY_ENGINE == 'ruby'
  if ENV.key?('COVERAGE')
    require 'simplecov'
    SimpleCov.start do
      add_group "Mongo", 'lib/mongo'
      add_group "BSON", 'lib/bson'
      add_filter "/test/"
      merge_timeout 3600
      command_name ENV['SIMPLECOV_COMMAND_NAME'] if ENV.has_key?('SIMPLECOV_COMMAND_NAME')
    end
  end
end
gem 'test-unit' # Do NOT remove this line - gem version is needed for Test::Unit::TestCase.shutdown
require 'test/unit'
require 'tools/mongo_config'

class Test::Unit::TestCase

  TEST_DATA = File.join(File.dirname(__FILE__), 'data')

  def ensure_cluster(kind=nil, opts={})
    @@cluster ||= nil

    unless @@cluster
      if kind == :rs
        cluster_opts = Mongo::Config::DEFAULT_REPLICA_SET.dup
      else
        cluster_opts = Mongo::Config::DEFAULT_SHARDED_SIMPLE.dup
      end

      cluster_opts.merge!(opts)

      dbpath = ENV['DBPATH'] || 'data'
      cluster_opts.merge!(:dbpath => dbpath)

      #debug 1, opts
      config = Mongo::Config.cluster(cluster_opts)
      #debug 1, config
      @@cluster = Mongo::Config::ClusterManager.new(config)

      Test::Unit::TestCase.class_eval do
        @@force_shutdown = false

        def self.shutdown
          if @@force_shutdown || /rake_test_loader/ !~ $0
            @@cluster.stop
            @@cluster.clobber
          end
        end
      end
    end

    @@cluster.start
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
  silently { require 'mocha/setup' }
rescue LoadError
  puts <<MSG

This test suite requires shoulda and mocha.
You can install them as follows:
  gem install shoulda
  gem install mocha

MSG
  exit
end

unless defined? MONGO_TEST_DB
  MONGO_TEST_DB = 'ruby-test-db'
end

unless defined? TEST_PORT
  TEST_PORT = ENV['MONGO_RUBY_DRIVER_PORT'] ? ENV['MONGO_RUBY_DRIVER_PORT'].to_i : Mongo::MongoClient::DEFAULT_PORT
end

unless defined? TEST_HOST
  TEST_HOST = ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
end

class Test::Unit::TestCase
  include Mongo
  include BSON

  def self.standard_connection(options={}, legacy=false)
    if legacy
      Connection.new(TEST_HOST, TEST_PORT, options)
    else
      MongoClient.new(TEST_HOST, TEST_PORT, options)
    end
  end

  def standard_connection(options={}, legacy=false)
    self.class.standard_connection(options, legacy)
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

  def method_name
    caller[0]=~/`(.*?)'/
    $1
  end

  def step_down_command
    # Adding force=true to avoid 'no secondaries within 10 seconds of my optime' errors
    step_down_command = BSON::OrderedHash.new
    step_down_command[:replSetStepDown] = 5
    step_down_command[:force]           = true
    step_down_command
  end

  def new_mock_socket(host='localhost', port=27017)
    socket = Object.new
    socket.stubs(:setsockopt).with(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
    socket.stubs(:close)
    socket.stubs(:closed?)
    socket.stubs(:checkin)
    socket.stubs(:pool)
    socket
  end

  def new_mock_unix_socket(sockfile='/tmp/mongod.sock')
    socket = Object.new
    socket.stubs(:setsockopt).with(Socket::IPPROTO_TCP)
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
