require 'mongo'
require 'rspec/autorun'

RSpec.configure do |config|
  config.color     = true
  config.fail_fast = true
  config.formatter = 'documentation'
  config.alias_it_should_behave_like_to :it_can_be, "it can be"
end