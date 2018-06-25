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
TRANSACTIONS_TESTS = Dir.glob("#{CURRENT_PATH}/support/transactions_tests/*.yml")

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
begin
  require 'byebug'
rescue LoadError
end

Mongo::Logger.logger = Logger.new($stdout)
unless %w(1 true yes).include?((ENV['CLIENT_DEBUG'] || '').downcase)
  Mongo::Logger.logger.level = Logger::INFO
end
Encoding.default_external = Encoding::UTF_8

require 'support/matchers'
require 'support/event_subscriber'
require 'support/server_discovery_and_monitoring'
require 'support/server_selection_rtt'
require 'support/server_selection'
require 'support/sdam_monitoring'
require 'support/crud'
require 'support/command_monitoring'
require 'support/connection_string'
require 'support/gridfs'
require 'support/transactions'

RSpec.configure do |config|
  if ENV['CI'] && RUBY_PLATFORM =~ /\bjava\b/
    config.formatter = 'documentation'
  end
end
