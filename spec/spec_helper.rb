if RUBY_VERSION > '1.9' && RUBY_VERSION < '2.2'
  require 'simplecov'
  require 'coveralls'

  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter[
    SimpleCov::Formatter::HTMLFormatter,
    Coveralls::SimpleCov::Formatter
  ]

  SimpleCov.start do
    # report groups
    add_group 'Wire Protocol', 'lib/mongo/protocol'

    # filters
    add_filter 'tasks'
    add_filter 'spec'
    add_filter 'bin'
  end
end

require 'mongo'
require 'support/helpers'
require 'support/matchers'
require 'support/monitoring'
require 'support/cluster_simulator'

Mongo::Logger.logger = Logger.new($stdout, Logger::DEBUG)
# Mongo::Logger.logger = Logger.new(StringIO.new, Logger::DEBUG)

RSpec.configure do |config|
  config.color     = true
  config.fail_fast = true unless ENV['CI'] || ENV['JENKINS_HOME']
  config.formatter = 'documentation'
  config.include Helpers
  config.include ClusterSimulator::Helpers
  ClusterSimulator.configure(config)

  config.after do
    Mongo::Server::Monitor.threads.each do |object_id, thread|
      thread.kill
    end
  end

  directory = File.expand_path(File.dirname(__FILE__))

  config.before(:suite) do
    user    = Mongo::Auth::User.new(TEST_DB, TEST_USER, TEST_PASSWORD)
    options = {
      pwd: user.hashed_password,
      roles: [ 'dbAdminAnyDatabase', 'userAdminAnyDatabase', 'readWriteAnyDatabase' ]
    }
    command_24 = { addUser: user.name }.merge(options)
    command_26 = { createUser: user.name }.merge(options)
    query_24 = Mongo::Protocol::Query.new('admin', '$cmd', command_24, :limit => -1)
    query_26 = Mongo::Protocol::Query.new('admin', '$cmd', command_26, :limit => -1)
    address = Mongo::Server::Address.new('127.0.0.1:27017')
    connection = Mongo::Connection.new(address)

    p connection.dispatch([ query_24 ])
    p connection.dispatch([ query_26 ])
  end
end

TEST_DB       = 'ruby-driver'
TEST_COLL     = 'test'
TEST_SET      = 'ruby-driver-rs'
TEST_USER     = 'test-user'
TEST_PASSWORD = 'password'
COVERAGE_MIN  = 90

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
