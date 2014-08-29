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

  config.before(:suite) do

    admin_client = Mongo::Client.new([ '127.0.0.1:27017' ], database: Mongo::Database::ADMIN).tap do |client|
      client.cluster.scan!
    end
    test_client = Mongo::Client.new([ '127.0.0.1:27017' ], database: TEST_DB).tap do |client|
      client.cluster.scan!
    end

    begin
      admin_client.database.users.create(ROOT_USER.name, ROOT_USER.password, roles: ROOT_USER.roles)
    rescue Exception; end
    begin
      test_client.database.users.create(ROOT_USER.name, ROOT_USER.password, roles: ROOT_USER.roles)
    rescue Exception; end
  end
end

TEST_DB         = 'ruby-driver'
TEST_COLL       = 'test'
TEST_SET        = 'ruby-driver-rs'
COVERAGE_MIN    = 90

ROOT_USER = Mongo::Auth::User.new(
  database: 'admin',
  user: 'root-user',
  password: 'password',
  roles: [
    Mongo::Auth::Roles::ROOT,
    Mongo::Auth::Roles::USER_ADMIN_ANY_DATABASE,
    Mongo::Auth::Roles::READ_WRITE
  ]
)

def write_command_enabled?
  @client ||= initialize_scanned_client!
  @write_command_enabled ||= @client.cluster.servers.first.write_command_enabled?
end

def initialize_scanned_client!
  client = Mongo::Client.new([ '127.0.0.1:27017' ], database: TEST_DB)
  client.cluster.scan!
  client
end

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
