COVERAGE_MIN = 90
CURRENT_PATH = File.expand_path(File.dirname(__FILE__))
SERVER_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/sdam/**/*.yml")
SDAM_MONITORING_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/sdam_monitoring/*.yml")
SERVER_SELECTION_RTT_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/server_selection_rtt/*.yml")
SERVER_SELECTION_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/server_selection/**/*.yml")
MAX_STALENESS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/max_staleness/**/*.yml")
CRUD_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/crud/**/*.yml")
RETRYABLE_WRITES_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/retryable_writes/**/*.yml")
COMMAND_MONITORING_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/command_monitoring/**/*.yml")
CONNECTION_STRING_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/connection_string/*.yml")
URI_OPTIONS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/uri_options/*.yml")
DNS_SEEDLIST_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/dns_seedlist_discovery/*.yml")
GRIDFS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/gridfs/*.yml")
TRANSACTIONS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/transactions/*.yml")
TRANSACTIONS_API_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/transactions_api/*.yml")
CHANGE_STREAMS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/change_streams/*.yml")
CMAP_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/cmap/*.yml")

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

unless ENV['CI']
  begin
    require 'byebug'
  rescue LoadError
    # jruby - try pry
    begin
      require 'pry'
    # jruby likes to raise random error classes, in this case
    # NameError in addition to LoadError
    rescue Exception
    end
  end
end

require 'support/spec_config'

Mongo::Logger.logger = Logger.new($stdout)
unless SpecConfig.instance.client_debug?
  Mongo::Logger.logger.level = Logger::INFO
end
Encoding.default_external = Encoding::UTF_8

autoload :Timecop, 'timecop'

require 'ice_nine'
require 'support/matchers'
require 'support/lite_constraints'
require 'support/event_subscriber'
require 'support/sdam'
require 'support/server_selection_rtt'
require 'support/server_selection'
require 'support/sdam_monitoring'
require 'support/crud'
require 'support/command_monitoring'
require 'support/cmap'
require 'support/connection_string'
require 'support/gridfs'
require 'support/transactions'
require 'support/change_streams'
require 'support/common_shortcuts'
require 'support/client_registry'
require 'support/client_registry_macros'
require 'support/json_ext_formatter'
require 'support/sdam_formatter_integration'

if SpecConfig.instance.mri?
  require 'timeout_interrupt'
else
  require 'timeout'
  TimeoutInterrupt = Timeout
end

RSpec.configure do |config|
  config.extend(CommonShortcuts)
  config.extend(LiteConstraints)
  config.include(ClientRegistryMacros)

  if SpecConfig.instance.ci?
    SdamFormatterIntegration.subscribe
    config.add_formatter(JsonExtFormatter, File.join(File.dirname(__FILE__), '../tmp/rspec.json'))

    config.around(:each) do |example|
      SdamFormatterIntegration.assign_log_entries(nil)
      begin
        example.run
      ensure
        SdamFormatterIntegration.assign_log_entries(example.id)
      end
    end
  end

  if SpecConfig.instance.ci?
    # Allow a max of 30 seconds per test.
    # Tests should take under 10 seconds ideally but it seems
    # we have some that run for more than 10 seconds in CI.
    config.around(:each) do |example|
      TimeoutInterrupt.timeout(45) do
        example.run
      end

      # To avoid a buildup of connections, we periodically close and reconnect each client if the
      # tests are running against a server.
      if defined?(NON_LITE_SPEC_TESTS) && NON_LITE_SPEC_TESTS && rand < 0.01
        close_local_clients(true)
      end
    end
  end
end

EventSubscriber.initialize

if SpecConfig.instance.active_support?
  require "active_support/time"
  require 'mongo/active_support'
end

# Converts a 'camelCase' string or symbol to a :snake_case symbol.
def camel_to_snake(ident)
  ident = ident.is_a?(String) ? ident.dup : ident.to_s
  ident[0] = ident[0].downcase
  ident.chars.reduce('') { |s, c| s + (/[A-Z]/ =~ c ? "_#{c.downcase}" : c) }.to_sym
end

# Creates a copy of a hash where all keys and string values are converted to snake-case symbols.
# For example, `{ 'fooBar' => { 'baz' => 'bingBing', :x => 1 } }` converts to
# `{ :foo_bar => { :baz => :bing_bing, :x => 1 } }`.
def snakeize_hash(value)
  return camel_to_snake(value) if value.is_a?(String)
  return value unless value.is_a?(Hash)

  value.reduce({}) do |hash, kv|
    hash.tap do |h|
      h[camel_to_snake(kv.first)] = snakeize_hash(kv.last)
    end
  end
end
