$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems' if ENV['C_EXT']
require 'mongo'
require 'test/unit'

begin
  require 'rubygems'
  require 'shoulda'
  require 'mocha'
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
  MONGO_TEST_DB = 'mongo-ruby-test'
end

class Test::Unit::TestCase
  include Mongo
  include BSON

  def self.standard_connection(options={})
    Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
      ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT, options)
  end

  def standard_connection(options={})
    self.class.standard_connection(options)
  end

  def self.host_port
    "#{mongo_host}:#{mongo_port}"
  end

  def self.mongo_host
    ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost'
  end

  def self.mongo_port
    ENV['MONGO_RUBY_DRIVER_PORT'] ? ENV['MONGO_RUBY_DRIVER_PORT'].to_i : 27017
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

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure
    success = false
    while !success
      begin
        yield
        success = true
      rescue Mongo::ConnectionFailure
        puts "Rescuing"
        sleep(1)
      end
    end
  end

  def assert_raise_error(klass, message)
    begin
      yield
    rescue => e
      assert_equal klass, e.class
      assert e.message.include?(message), "#{e.message} does not include #{message}."
    else
      flunk "Expected assertion #{klass} but none was raised."
    end
  end
end
