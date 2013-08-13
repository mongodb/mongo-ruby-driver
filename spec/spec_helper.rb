unless RUBY_VERSION < '1.9'
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
require 'rspec/autorun'

RSpec.configure do |config|
  config.color     = true
  config.fail_fast = true unless ENV['CI']
  config.formatter = 'documentation'
  config.treat_symbols_as_metadata_keys_with_true_values = true
  config.include Helpers

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
