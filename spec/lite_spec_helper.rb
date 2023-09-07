# frozen_string_literal: true
# rubocop:todo all

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), "shared", "lib"))

COVERAGE_MIN = 90
CURRENT_PATH = File.expand_path(File.dirname(__FILE__))

SERVER_DISCOVERY_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/sdam/**/*.yml").sort
SDAM_MONITORING_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/sdam_monitoring/*.yml").sort
SERVER_SELECTION_RTT_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/server_selection_rtt/*.yml").sort
CRUD_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/crud/**/*.yml").sort
CONNECTION_STRING_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/connection_string/*.yml").sort
URI_OPTIONS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/uri_options/*.yml").sort
GRIDFS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/gridfs/*.yml").sort
TRANSACTIONS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/transactions/*.yml").sort
TRANSACTIONS_API_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/transactions_api/*.yml").sort
CHANGE_STREAMS_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/change_streams/*.yml").sort
CMAP_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/cmap/*.yml").sort.select do |f|
  # Skip tests that are flaky on JRuby.
  # https://jira.mongodb.org/browse/RUBY-3292
  !defined?(JRUBY_VERSION) || !f.include?('pool-checkout-minPoolSize-connection-maxConnecting.yml')
end
AUTH_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/auth/*.yml").sort
CLIENT_SIDE_ENCRYPTION_TESTS = Dir.glob("#{CURRENT_PATH}/spec_tests/data/client_side_encryption/*.yml").sort

# Disable output buffering: https://www.rubyguides.com/2019/02/ruby-io/
STDOUT.sync = true
STDERR.sync = true

if %w(1 true yes).include?(ENV['CI']&.downcase)
  autoload :Byebug, 'byebug'
else
  # Load debuggers before loading the driver code, so that breakpoints
  # can be placed in the driver code on file/class level.
  begin
    require 'byebug'
  rescue LoadError
    begin
      require 'ruby-debug'
    rescue LoadError
    end
  end
end

require 'mongo'
require 'pp'

if BSON::Environment.jruby?
  # Autoloading appears to not work in some environments without these
  # gem calls. May have to do with rubygems version?
  gem 'ice_nine'
  gem 'timecop'
end

autoload :Benchmark, 'benchmark'
autoload :IceNine, 'ice_nine'
autoload :Timecop, 'timecop'
autoload :ChildProcess, 'childprocess'

require 'rspec/retry'

if BSON::Environment.jruby?
  require 'concurrent-ruby'
  PossiblyConcurrentArray = Concurrent::Array
else
  PossiblyConcurrentArray = Array
end

require 'support/utils'
require 'support/spec_config'

Mongo::Logger.logger = Logger.new(STDOUT)
unless SpecConfig.instance.client_debug?
  Mongo::Logger.logger.level = Logger::INFO
end
Encoding.default_external = Encoding::UTF_8

module Mrss
  autoload :Utils, 'mrss/utils'
end

require 'mrss/lite_constraints'
require 'support/matchers'
require 'mrss/event_subscriber'
require 'support/common_shortcuts'
require 'support/client_registry'
require 'support/client_registry_macros'
require 'support/mongos_macros'
require 'support/macros'
require 'support/crypt'
require 'support/json_ext_formatter'
require 'support/sdam_formatter_integration'
require 'support/background_thread_registry'
require 'mrss/session_registry'
require 'support/local_resource_registry'

if SpecConfig.instance.mri? && !SpecConfig.instance.windows?
  require 'timeout_interrupt'
else
  require 'timeout'
  TimeoutInterrupt = Timeout
end

Mrss.patch_mongo_for_session_registry

class ExampleTimeout < StandardError; end

STANDARD_TIMEOUTS = {
  stress: 210,
  jruby: 90,
  default: 45,
}.freeze

def timeout_type
  if ENV['EXAMPLE_TIMEOUT'].to_i > 0
    :custom
  elsif %w(1 true yes).include?(ENV['STRESS']&.downcase)
    :stress
  elsif BSON::Environment.jruby?
    :jruby
  else
    :default
  end
end

def example_timeout_seconds
  STANDARD_TIMEOUTS.fetch(
    timeout_type,
    (ENV['EXAMPLE_TIMEOUT'] || STANDARD_TIMEOUTS[:default]).to_i
  )
end

RSpec.configure do |config|
  config.extend(CommonShortcuts::ClassMethods)
  config.include(CommonShortcuts::InstanceMethods)
  config.extend(Mrss::LiteConstraints)
  config.include(ClientRegistryMacros)
  config.include(MongosMacros)
  config.extend(Mongo::Macros)

  # Used for spec/solo/*
  def require_solo
    before(:all) do
      unless %w(1 true yes).include?(ENV['SOLO'])
        skip 'Set SOLO=1 in environment to run solo tests'
      end
    end
  end

  def require_atlas
    before do
      skip 'Set ATLAS_URI in environment to run atlas tests' if ENV['ATLAS_URI'].nil?
    end
  end

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

  if SpecConfig.instance.ci? && !%w(1 true yes).include?(ENV['INTERACTIVE']&.downcase)
    # Tests should take under 10 seconds ideally but it seems
    # we have some that run for more than 10 seconds in CI.
    config.around(:each) do |example|
      TimeoutInterrupt.timeout(example_timeout_seconds, ExampleTimeout) do
        example.run
      end
    end
  end

  if SpecConfig.instance.ci?
    if defined?(Rfc::Rif)
      unless BSON::Environment.jruby?
        Rfc::Rif.output_object_space_stats = true
      end

      # Uncomment this line to log memory and CPU statistics during
      # test suite execution to diagnose issues potentially related to
      # system resource exhaustion.
      #Rfc::Rif.output_system_load = true
    end
  end

  config.expect_with :rspec do |c|
    c.syntax = [:should, :expect]
    c.max_formatted_output_length = 10000
  end

  if config.respond_to?(:fuubar_output_pending_results=)
    config.fuubar_output_pending_results = false
  end
end

if SpecConfig.instance.active_support?
  require "active_support/version"
  if ActiveSupport.version >= Gem::Version.new(7)
    # ActiveSupport wants us to require ALL of it all of the time.
    # See: https://github.com/rails/rails/issues/43851,
    # https://github.com/rails/rails/issues/43889, etc.
    require 'active_support'
  end
  require "active_support/time"
  require 'mongo/active_support'
end

if File.exist?('.env.private')
  require 'dotenv'
  Dotenv.load('.env.private')
end
