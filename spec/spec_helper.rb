if RUBY_VERSION > '1.9'
  require 'coveralls'
  Coveralls.wear!
end

require 'mongo'
require 'support/helpers'
require 'rspec/autorun'

RSpec.configure do |config|
  config.color     = true
  config.fail_fast = true unless ENV['CI']
  config.formatter = 'documentation'
  config.include Helpers
end
