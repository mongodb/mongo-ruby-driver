$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
require 'rubygems' if ENV['C_EXT']
require 'mongo'
require 'test/unit'

begin
  require 'rubygems'
  require 'shoulda'
  require 'mocha'
  rescue LoadError
    puts <<MSG

This test suite requires shoulda and mocha.
You can install them as follows:
  gem install shoulda
  gem install mocha

MSG
    exit
end

require 'bson_ext/cbson' if ENV['C_EXT']

MONGO_TEST_DB = 'mongo-ruby-test'

# NOTE: most tests assume that MongoDB is running.
class Test::Unit::TestCase
  include Mongo
  include BSON

  # Generic code for rescuing connection failures and retrying operations.
  # This could be combined with some timeout functionality.
  def rescue_connection_failure
    success = false
    while !success
      begin
        yield
        success = true
      rescue Mongo::ConnectionFailure
        puts "Rescuing"
        sleep(1)
      end
    end
  end
end
