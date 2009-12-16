# Copyright 2009 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Exceptions raised by the MongoDB driver.

module Mongo
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

  # Raised when trying to insert a document that exceeds the 4MB limit.
  class InvalidDocument < MongoDBError; end

  # Raised when a database operation fails.
  class OperationFailure < MongoDBError; end

  # Raised when a database operation fails.
  class ConnectionFailure < MongoDBError; end

  # Raised when a client attempts to perform an invalid operation.
  class InvalidOperation < MongoDBError; end

  # Raised when an invalid name is used.
  class InvalidName < RuntimeError; end

  # Raised when the client supplies an invalid value to sort by.
  class InvalidSortValueError < MongoRubyError; end
end
