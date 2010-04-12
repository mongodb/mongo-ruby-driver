$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

module BSON
  VERSION = "0.20.2"
  def self.serialize(obj, check_keys=false, move_id=false)
    BSON_CODER.serialize(obj, check_keys, move_id)
  end


  def self.deserialize(buf=nil)
    BSON_CODER.deserialize(buf)
  end
end

begin
  # Need this for running test with and without c ext in Ruby 1.9.
  raise LoadError if ENV['TEST_MODE'] && !ENV['C_EXT']
  require 'bson_ext/cbson'
  raise LoadError unless defined?(CBson::VERSION) && CBson::VERSION == BSON::VERSION
  require 'bson/bson_c'
  module BSON
    BSON_CODER = BSON_C
  end
rescue LoadError
  require 'bson/bson_ruby'
  module BSON
    BSON_CODER = BSON_RUBY
  end
  warn "\n**Notice: C extension not loaded. This is required for optimum MongoDB Ruby driver performance."
  warn "  You can install the extension as follows:\n  gem install bson_ext\n"
  warn "  If you continue to receive this message after installing, make sure that the"
  warn "  bson_ext gem is in your load path and that the bson_ext and mongo gems are of the same version.\n"
end

require 'bson/types/binary'
require 'bson/types/code'
require 'bson/types/dbref'
require 'bson/types/objectid'
require 'bson/types/min_max_keys'

require 'base64'
require 'bson/ordered_hash'
require 'bson/byte_buffer'
require 'bson/bson_ruby'
require 'bson/exceptions'
