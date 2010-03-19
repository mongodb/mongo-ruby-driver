$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

module Mongo
  module BSON
    VERSION = "0.19.2"
  end
end

begin
  # Need this for running test with and without c ext in Ruby 1.9.
  raise LoadError if ENV['TEST_MODE'] && !ENV['C_EXT']
  require 'mongo_ext/cbson'
  raise LoadError unless defined?(CBson::VERSION) && CBson::VERSION == Mongo::BSON::VERSION
  require 'mongo_bson/bson_c'
  module Mongo
    BSON_CODER = BSON_C
  end
rescue LoadError
  require 'mongo_bson/bson_ruby'
  module Mongo
    BSON_CODER = BSON_RUBY
  end
  warn "\n**Notice: C extension not loaded. This is required for optimum MongoDB Ruby driver performance."
  warn "  You can install the extension as follows:\n  gem install mongo_ext\n"
  warn "  If you continue to receive this message after installing, make sure that the"
  warn "  mongo_ext gem is in your load path and that the mongo_ext and mongo gems are of the same version.\n"
end

require 'mongo_bson/types/binary'
require 'mongo_bson/types/code'
require 'mongo_bson/types/dbref'
require 'mongo_bson/types/objectid'
require 'mongo_bson/types/regexp_of_holding'
require 'mongo_bson/types/min_max_keys'

require 'base64'
require 'mongo_bson/ordered_hash'
require 'mongo_bson/byte_buffer'
require 'mongo_bson/bson_ruby'
require 'mongo_bson/exceptions'
