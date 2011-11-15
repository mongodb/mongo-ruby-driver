require File.join(File.dirname(__FILE__), '..', '..', 'lib', 'bson')
require 'rubygems' if RUBY_VERSION < '1.9.0' && ENV['C_EXT']
require 'test/unit'

def silently
  warn_level = $VERBOSE
  $VERBOSE = nil
  result = yield
  $VERBOSE = warn_level
  result
end

require 'bson_ext/cbson' if !(RUBY_PLATFORM =~ /java/) && ENV['C_EXT']

class Test::Unit::TestCase
  include BSON

  def assert_raise_error(klass, message)
    begin
      yield
    rescue => e
      assert_equal klass, e.class
      assert e.message.include?(message), "#{e.message} does not include #{message}."
    else
      flunk "Expected assertion #{klass} but none was raised."
    end
  end

end
