module Mongo
  # Generic Mongo Ruby Driver exception class.
  class MongoRubyError < StandardError; end

  # Raised when MongoDB itself has returned an error.
  class MongoDBError < RuntimeError

     # @return The entire failed command's response object, if available.
     attr_reader :result

     # @return The failed command's error code, if availab.e
     attr_reader :error_code

     def initialize(message=nil, error_code=nil, result=nil)
       @error_code = error_code
       @result = result
       super(message)
     end
  end

  # Raised on fatal errors to GridFS.
  class GridError < MongoRubyError; end

  # Raised on fatal errors to GridFS.
  class GridFileNotFound < GridError; end

  # Raised on fatal errors to GridFS.
  class GridMD5Failure < GridError; end

  # Raised when invalid arguments are sent to Mongo Ruby methods.
  class MongoArgumentError < MongoRubyError; end

  # Raised on failures in connection to the database server.
  class ConnectionError < MongoRubyError; end

  # Raised on failures in connection to the database server.
  class ReplicaSetConnectionError < ConnectionError; end

  # Raised on failures in connection to the database server.
  class ConnectionTimeoutError < MongoRubyError; end

  # Raised when no tags in a read preference maps to a given connection.
  class NodeWithTagsNotFound < MongoRubyError; end

  # Raised when a connection operation fails.
  class ConnectionFailure < MongoDBError; end

  # Raised when authentication fails.
  class AuthenticationError < MongoDBError; end

  # Raised when a database operation fails.
  class OperationFailure < MongoDBError; end

  # Raised when a socket read operation times out.
  class OperationTimeout < SocketError; end

  # Raised when a client attempts to perform an invalid operation.
  class InvalidOperation < MongoDBError; end

  # Raised when an invalid collection or database name is used (invalid namespace name).
  class InvalidNSName < RuntimeError; end

  # Raised when the client supplies an invalid value to sort by.
  class InvalidSortValueError < MongoRubyError; end
end
