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

require 'support/matchers'
require 'support/authorization'
require 'support/mongo_orchestration'
require 'support/server_discovery_and_monitoring'

Mongo::Logger.logger = Logger.new($stdout)
Mongo::Logger.logger.level = Logger::INFO

RSpec.configure do |config|
  config.color     = true
  config.fail_fast = true unless ENV['CI'] || ENV['JENKINS_HOME']
  config.formatter = 'documentation'
  config.include(Authorization)

  config.before(:suite) do
    begin
      # Create the root user administrator as the first user to be added to the
      # database. This user will need to be authenticated in order to add any
      # more users to any other databases.
      p ADMIN_UNAUTHORIZED_CLIENT.database.users.create(ROOT_USER)
    rescue Exception => e
      p e
    end
    begin
      # Adds the test user to the test database with permissions on all
      # databases that will be used in the test suite.
      p ADMIN_AUTHORIZED_CLIENT.database.users.create(TEST_USER)
    rescue Exception => e
      p e
      unless write_command_enabled?
        # If we are on versions less than 2.6, we need to create a user for
        # each database, since the users are not stored in the admin database
        # but in the system.users collection on the datbases themselves. Also,
        # roles in versions lower than 2.6 can only be strings, not hashes.
        begin p ROOT_AUTHORIZED_CLIENT.database.users.create(TEST_READ_WRITE_USER); rescue; end
      end
    end
  end
end

TEST_SET = 'ruby-driver-rs'
COVERAGE_MIN = 90
CURRENT_PATH = File.expand_path(File.dirname(__FILE__))
SERVER_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/support/sdam/**/*.yml")

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 2.6 or higher.
#
# @since 2.0.0
def write_command_enabled?
  @client ||= initialize_scanned_client!
  @write_command_enabled ||= @client.cluster.servers.first.features.write_command_enabled?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 2.7 or higher.
#
# @since 2.0.0
def list_command_enabled?
  @client ||= initialize_scanned_client!
  @list_command_enabled ||= @client.cluster.servers.first.features.list_indexes_enabled?
end

alias :scram_sha_1_enabled? :list_command_enabled?

# Depending on whether write commands are enabled, there are different documents that
# are guaranteed to cause a delete failure.
#
# @since 2.0.0
def failing_delete_doc
  write_command_enabled? ? { q: { '$set' => { a: 1 } }, limit: 0 } :
                           { que: { field: 'test' } }
end

# Initializes a basic scanned client to do an ismaster check.
#
# @since 2.0.0
def initialize_scanned_client!
  Mongo::Client.new([ '127.0.0.1:27017' ], database: TEST_DB)
end

def initialize_mo_standalone!(path = nil)
  @standalone ||= MongoOrchestration.get(:standalone, path: path)
end

def stop_mo_standalone!
  @standalone.stop if @standalone
end

def mongo_orchestration_available?(path = nil)
  begin
    MongoOrchestration.get(:standalone, path: path)
  rescue MongoOrchestration::ServiceNotAvailable
    return false
  end
end

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
