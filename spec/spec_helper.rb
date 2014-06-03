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
    add_group 'Connection Pool', 'lib/mongo/pool'

    # filters
    add_filter 'tasks'
    add_filter 'spec'
    add_filter 'bin'
  end
end

require 'mongo'
require 'support/helpers'
require 'support/matchers'
require 'support/cluster_simulator'

RSpec.configure do |config|
  config.color     = true
  config.fail_fast = true unless ENV['CI']
  config.formatter = 'documentation'
  config.include Helpers
  config.include ClusterSimulator::Helpers
  ClusterSimulator.configure(config)

  # disables 'should' syntax
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.mock_with :rspec do |c|
    c.syntax = :expect
  end
end

TEST_DB      = 'ruby-driver'
TEST_COLL    = 'test'
TEST_SET     = 'ruby-driver-rs'
COVERAGE_MIN = 90

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
