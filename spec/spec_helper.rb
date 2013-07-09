unless RUBY_VERSION < '1.9'
  require 'coveralls'
  Coveralls.wear!
end

require 'mongo'
require 'support/helpers'
require 'support/matchers'
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

TEST_DB      = 'ruby-driver'
TEST_COLL    = 'test'
COVERAGE_MIN = 99

# require all shared examples
Dir['./spec/support/shared/*.rb'].sort.each { |file| require file }
