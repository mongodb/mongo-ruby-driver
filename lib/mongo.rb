$:.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))

module Mongo
  VERSION = "0.18.2"
end

begin
    # Need this for running test with and without c ext in Ruby 1.9.
    raise LoadError if ENV['TEST_MODE'] && !ENV['C_EXT']
    require 'mongo_ext/cbson'
    raise LoadError unless defined?(CBson::VERSION) && CBson::VERSION == Mongo::VERSION
    require 'mongo/util/bson_c'
    BSON = BSON_C
  rescue LoadError
    require 'mongo/util/bson_ruby'
    BSON = BSON_RUBY
    warn "\n**Notice: C extension not loaded. This is required for optimum MongoDB Ruby driver performance."
    warn "  You can install the extension as follows:\n  gem install mongo_ext\n"
    warn "  If you continue to receive this message after installing, make sure that the"
    warn "  mongo_ext gem is in your load path and that the mongo_ext and mongo gems are of the same version.\n"
end

module Mongo
  ASCENDING = 1
  DESCENDING = -1

  module Constants
    OP_REPLY        = 1
    OP_MSG          = 1000
    OP_UPDATE       = 2001
    OP_INSERT       = 2002
    OP_QUERY        = 2004
    OP_GET_MORE     = 2005
    OP_DELETE       = 2006
    OP_KILL_CURSORS = 2007

    OP_QUERY_SLAVE_OK          = 4
    OP_QUERY_NO_CURSOR_TIMEOUT = 16
  end

  # Generic Mongo Ruby Driver exception class.
  class MongoRubyError < StandardError; end

  # Raised when MongoDB itself has returned an error.
  class MongoDBError < RuntimeError; end

  # Raised when configuration options cause connections, queries, etc., to fail.
  class ConfigurationError < MongoRubyError; end

  # Raised when invalid arguments are sent to Mongo Ruby methods.
  class MongoArgumentError < MongoRubyError; end

  # Raised when given a string is not valid utf-8 (Ruby 1.8 only).
  class InvalidStringEncoding < MongoRubyError; end

  # Raised when attempting to initialize an invalid ObjectID.
  class InvalidObjectID < MongoRubyError; end

  # Raised on failures in connection to the database server.
  class ConnectionError < MongoRubyError; end

  # Raised on failures in connection to the database server.
  class ConnectionTimeoutError < MongoRubyError; end

  # Raised when trying to insert a document that exceeds the 4MB limit or
  # when the document contains objects that can't be serialized as BSON.
  class InvalidDocument < MongoDBError; end

  # Raised when a database operation fails.
  class OperationFailure < MongoDBError; end

  # Raised when a connection operation fails.
  class ConnectionFailure < MongoDBError; end

  # Raised when a client attempts to perform an invalid operation.
  class InvalidOperation < MongoDBError; end

  # Raised when an invalid name is used.
  class InvalidName < RuntimeError; end

  # Raised when the client supplies an invalid value to sort by.
  class InvalidSortValueError < MongoRubyError; end
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

require 'mongo/connection'
require 'mongo/db'
require 'mongo/cursor'
require 'mongo/collection'
require 'mongo/admin'
require 'mongo/gridfs'
