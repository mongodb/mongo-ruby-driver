TEST_SET = 'ruby-driver-rs'
COVERAGE_MIN = 90
CURRENT_PATH = File.expand_path(File.dirname(__FILE__))
SERVER_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/support/sdam/**/*.yml")
SDAM_MONITORING_TESTS = Dir.glob("#{CURRENT_PATH}/support/sdam_monitoring/*.yml")
SERVER_SELECTION_RTT_TESTS = Dir.glob("#{CURRENT_PATH}/support/server_selection/rtt/*.yml")
SERVER_SELECTION_TESTS = Dir.glob("#{CURRENT_PATH}/support/server_selection/selection/**/*.yml")
MAX_STALENESS_TESTS = Dir.glob("#{CURRENT_PATH}/support/max_staleness/**/*.yml")
CRUD_TESTS = Dir.glob("#{CURRENT_PATH}/support/crud_tests/**/*.yml")
COMMAND_MONITORING_TESTS = Dir.glob("#{CURRENT_PATH}/support/command_monitoring/**/*.yml")
CONNECTION_STRING_TESTS = Dir.glob("#{CURRENT_PATH}/support/connection_string_tests/*.yml")
GRIDFS_TESTS = Dir.glob("#{CURRENT_PATH}/support/gridfs_tests/*.yml")

if ENV['DRIVERS_TOOLS']
  CLIENT_CERT_PEM = ENV['DRIVER_TOOLS_CLIENT_CERT_PEM']
  CLIENT_KEY_PEM = ENV['DRIVER_TOOLS_CLIENT_KEY_PEM']
  CA_PEM = ENV['DRIVER_TOOLS_CA_PEM']
  CLIENT_KEY_ENCRYPTED_PEM = ENV['DRIVER_TOOLS_CLIENT_KEY_ENCRYPTED_PEM']
else
  SSL_CERTS_DIR = "#{CURRENT_PATH}/support/certificates"
  CLIENT_PEM = "#{SSL_CERTS_DIR}/client.pem"
  CLIENT_PASSWORD_PEM = "#{SSL_CERTS_DIR}/password_protected.pem"
  CA_PEM = "#{SSL_CERTS_DIR}/ca.pem"
  CRL_PEM = "#{SSL_CERTS_DIR}/crl.pem"
  CLIENT_KEY_PEM = "#{SSL_CERTS_DIR}/client_key.pem"
  CLIENT_CERT_PEM = "#{SSL_CERTS_DIR}/client_cert.pem"
  CLIENT_KEY_ENCRYPTED_PEM = "#{SSL_CERTS_DIR}/client_key_encrypted.pem"
  CLIENT_KEY_PASSPHRASE = "passphrase"
end

require 'mongo'

Mongo::Logger.logger = Logger.new($stdout)
Mongo::Logger.logger.level = Logger::INFO
Encoding.default_external = Encoding::UTF_8

require 'support/travis'
require 'support/matchers'
require 'support/authorization'
require 'support/server_discovery_and_monitoring'
require 'support/server_selection_rtt'
require 'support/server_selection'
require 'support/sdam_monitoring'
require 'support/crud'
require 'support/command_monitoring'
require 'support/connection_string'
require 'support/gridfs'

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
      ADMIN_UNAUTHORIZED_CLIENT.database.users.create(ROOT_USER)
      ADMIN_UNAUTHORIZED_CLIENT.close
    rescue Exception => e
    end
    begin
      # Adds the test user to the test database with permissions on all
      # databases that will be used in the test suite.
      ADMIN_AUTHORIZED_TEST_CLIENT.database.users.create(TEST_USER)
    rescue Exception => e
      unless write_command_enabled?
        # If we are on versions less than 2.6, we need to create a user for
        # each database, since the users are not stored in the admin database
        # but in the system.users collection on the databases themselves. Also,
        # roles in versions lower than 2.6 can only be strings, not hashes.
        begin ADMIN_AUTHORIZED_TEST_CLIENT.database.users.create(TEST_READ_WRITE_USER); rescue; end
      end
    end
  end
end

# Determine whether the test clients are connecting to a standalone.
#
# @since 2.0.0
def standalone?
  $mongo_client ||= initialize_scanned_client!
  $standalone ||= $mongo_client.cluster.servers.first.standalone?
end

# Determine whether the test clients are connecting to a replica set.
#
# @since 2.0.0
def replica_set?
  $mongo_client ||= initialize_scanned_client!
  $replica_set ||= $mongo_client.cluster.replica_set?
end

# Determine whether the test clients are connecting to a sharded cluster
# or a single mongos.
#
# @since 2.0.0
def sharded?
  $mongo_client ||= initialize_scanned_client!
  $sharded ||= ($mongo_client.cluster.sharded? || single_mongos?)
end

# Determine whether the single address provided is a replica set member.
# @note To run the specs relying on this to return true,
#   start a replica set and set the environment variable
#   MONGODB_ADDRESSES to the address of a single member.
#
# @since 2.0.0
def single_rs_member?
  $mongo_client ||= initialize_scanned_client!
  $single_rs_member ||= (single_seed? &&
      $mongo_client.cluster.servers.first.replica_set_name)
end

# Determine whether the single address provided is a mongos.
# @note To run the specs relying on this to return true,
#   start a sharded cluster and set the environment variable
#   MONGODB_ADDRESSES to the address of a single mongos.
#
# @since 2.0.0
def single_mongos?
  $mongo_client ||= initialize_scanned_client!
  $single_mongos ||= (single_seed? &&
      $mongo_client.cluster.servers.first.mongos?)
end

# Determine whether a single address was provided.
#
# @since 2.0.0
def single_seed?
  ADDRESSES.size == 1
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 3.4 or higher.
#
# @since 2.4.0
def collation_enabled?
  $mongo_client ||= initialize_scanned_client!
  $collation_enabled ||= $mongo_client.cluster.servers.first.features.collation_enabled?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 3.2 or higher.
#
# @since 2.0.0
def find_command_enabled?
  $mongo_client ||= initialize_scanned_client!
  $find_command_enabled ||= $mongo_client.cluster.servers.first.features.find_command_enabled?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 2.6 or higher.
#
# @since 2.0.0
def write_command_enabled?
  $mongo_client ||= initialize_scanned_client!
  $write_command_enabled ||= $mongo_client.cluster.servers.first.features.write_command_enabled?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 2.7 or higher.
#
# @since 2.0.0
def list_command_enabled?
  $mongo_client ||= initialize_scanned_client!
  $list_command_enabled ||= $mongo_client.cluster.servers.first.features.list_indexes_enabled?
end

# Is the test suite running locally (not on Travis or Jenkins).
#
# @since 2.1.0
def testing_ssl_locally?
  running_ssl? && !(ENV['CI'] || ENV['JENKINS_CI'])
end

# Is the test suite running on SSL.
#
# @since 2.0.2
def running_ssl?
  SSL
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

# Try running a command on the admin database to see if the mongod was started with auth.
#
# @since 2.2.0
def auth_enabled?
  if auth = ENV['AUTH']
    auth == 'auth'
  else
    $mongo_client ||= initialize_scanned_client!
    begin
      $mongo_client.use(:admin).command(getCmdLineOpts: 1).first["argv"].include?("--auth")
    rescue => e
      e.message =~ /(not authorized)|(unauthorized)/
    end
  end
end

# Initializes a basic scanned client to do an ismaster check.
#
# @since 2.0.0
def initialize_scanned_client!
  Mongo::Client.new(ADDRESSES, TEST_OPTIONS.merge(database: TEST_DB))
end

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
