$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'rubygems'
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

require 'mongo'

# NOTE: most tests assume that MongoDB is running.
class Test::Unit::TestCase
  include Mongo
end
