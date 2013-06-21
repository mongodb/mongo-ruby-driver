if RUBY_VERSION > '1.9' && RUBY_PLATFORM != 'java'
  require 'coveralls'
  Coveralls.wear!
end

require 'mongo'
require 'support/helpers'
require 'support/matchers'
require 'support/shared_examples'
require 'rspec/autorun'

RSpec.configure do |config|
  config.color     = true
  config.fail_fast = true unless ENV['CI']
  config.formatter = 'documentation'
  config.include Helpers

  # disables should syntax
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

TEST_DB = 'ruby-driver'
TEST_COLL = 'test'
