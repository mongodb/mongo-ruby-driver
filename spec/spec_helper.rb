TEST_SET = 'ruby-driver-rs'
COVERAGE_MIN = 90
CURRENT_PATH = File.expand_path(File.dirname(__FILE__))
SERVER_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/support/sdam/**/*.yml")
SDAM_MONITORING_TESTS = Dir.glob("#{CURRENT_PATH}/support/sdam_monitoring/*.yml")
SERVER_SELECTION_RTT_TESTS = Dir.glob("#{CURRENT_PATH}/support/server_selection/rtt/*.yml")
SERVER_SELECTION_TESTS = Dir.glob("#{CURRENT_PATH}/support/server_selection/selection/**/*.yml")
MAX_STALENESS_TESTS = Dir.glob("#{CURRENT_PATH}/support/max_staleness/**/*.yml")
CRUD_TESTS = Dir.glob("#{CURRENT_PATH}/support/crud_tests/**/*.yml")
RETRYABLE_WRITES_TESTS = Dir.glob("#{CURRENT_PATH}/support/retryable_writes_tests/**/*.yml")
COMMAND_MONITORING_TESTS = Dir.glob("#{CURRENT_PATH}/support/command_monitoring/**/*.yml")
CONNECTION_STRING_TESTS = Dir.glob("#{CURRENT_PATH}/support/connection_string_tests/*.yml")
DNS_SEEDLIST_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/support/dns_seedlist_discovery_tests/*.yml")
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
require 'support/event_subscriber'
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
  config.fail_fast = true unless ENV['CI']
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
# determine in the specs if we are 3.6 or higher.
#
# @since 2.5.0
def op_msg_enabled?
  $mongo_client ||= initialize_scanned_client!
  $op_msg_enabled ||= $mongo_client.cluster.servers.first.features.op_msg_enabled?
end
alias :change_stream_enabled? :op_msg_enabled?
alias :sessions_enabled? :op_msg_enabled?


# Whether sessions can be tested. Sessions are available on server versions 3.6
#   and higher and when connected to a replica set or sharded cluster.
#
# @since 2.5.0
def test_sessions?
  sessions_enabled? && (replica_set? || sharded?)
end

# Whether change streams can be tested. Change streams are available on server versions 3.6
#   and higher and when connected to a replica set.
#
# @since 2.5.0
def test_change_streams?
  !BSON::Environment.jruby? && change_stream_enabled? & replica_set?
end

# For instances where behaviour is different on different versions, we need to
# determine in the specs if we are 3.6 or higher.
#
# @since 2.5.0
def array_filters_enabled?
  $mongo_client ||= initialize_scanned_client!
  $array_filters_enabled ||= $mongo_client.cluster.servers.first.features.array_filters_enabled?
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
# determine in the specs if we are 2.7 or higher.
#
# @since 2.0.0
def list_command_enabled?
  $mongo_client ||= initialize_scanned_client!
  $list_command_enabled ||= $mongo_client.cluster.servers.first.features.list_indexes_enabled?
end

# Is the test suite running locally (not on Travis).
#
# @since 2.1.0
def testing_ssl_locally?
  running_ssl? && !(ENV['CI'])
end

# Should tests relying on external connections be run.
#
# @since 2.5.1
def test_connecting_externally?
  !ENV['CI'] && !ENV['EXTERNAL_DISABLED']
end

# Is the test suite running on SSL.
#
# @since 2.0.2
def running_ssl?
  SSL
end

# Is the test suite using compression.
#
# @since 2.5.0
def compression_enabled?
  COMPRESSORS[:compressors]
end

# Is the test suite testing compression.
# Requires that the server supports compression and compression is used by the test client.
#
# @since 2.5.0
def testing_compression?
  compression_enabled? && op_msg_enabled?
end

alias :scram_sha_1_enabled? :list_command_enabled?

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
