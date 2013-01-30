module BSON
  # Generic Mongo Ruby Driver exception class.
  class MongoRubyError < StandardError; end

  # Raised when MongoDB itself has returned an error.
  class MongoDBError < RuntimeError; end

  # This will replace MongoDBError.
  class BSONError < MongoDBError; end

  # Raised when given a string is not valid utf-8 (Ruby 1.8 only).
  class InvalidStringEncoding < BSONError; end

  # Raised when attempting to initialize an invalid ObjectId.
  class InvalidObjectId < BSONError; end

  # Raised when trying to insert a document that exceeds the 4MB limit or
  # when the document contains objects that can't be serialized as BSON.
  class InvalidDocument < BSONError; end

  # Raised when an invalid name is used.
  class InvalidKeyName < BSONError; end
end
