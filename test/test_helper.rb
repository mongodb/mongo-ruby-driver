$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'rubygems'
require 'mongo'
require 'test/unit'

begin
  require 'shoulda'
  require 'mocha'
  rescue LoadError
    puts <<MSG

This test suite now requires shoulda and mocha.
You can install these gems as follows:
  gem install shoulda
  gem install mocha

MSG
    exit
end

# NOTE: most tests assume that MongoDB is running.
class Test::Unit::TestCase
  include Mongo

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
