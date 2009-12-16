$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

module Mongo
  ASCENDING = 1
  DESCENDING = -1

  VERSION = "0.18.1"
end

begin
    # Need this for running test with and without c ext in Ruby 1.9.
    raise LoadError if ENV['TEST_MODE'] && !ENV['C_EXT']
    require 'mongo_ext/cbson'
    raise LoadError unless defined?(CBson::VERSION) && CBson::VERSION == Mongo::VERSION
    require 'mongo/util/bson_c'
    BSON            = BSON_C
  rescue LoadError
    require 'mongo/util/bson_ruby'
    BSON            = BSON_RUBY
    warn "\n**Notice: C extension not loaded. This is required for optimum MongoDB Ruby driver performance."
    warn "  You can install the extension as follows:\n  gem install mongo_ext\n"
    warn "  If you continue to receive this message after installing, make sure that the"
    warn "  mongo_ext gem is in your load path and that the mongo_ext and mongo gems are of the same version.\n"
end

require 'mongo/types/binary'
require 'mongo/types/code'
require 'mongo/types/dbref'
require 'mongo/types/objectid'
require 'mongo/types/regexp_of_holding'

require 'mongo/util/support'
require 'mongo/util/conversions'
require 'mongo/util/server_version'
require 'mongo/util/bson_ruby'

require 'mongo/errors'
require 'mongo/constants'
require 'mongo/connection'
require 'mongo/db'
require 'mongo/cursor'
require 'mongo/collection'
require 'mongo/admin'

