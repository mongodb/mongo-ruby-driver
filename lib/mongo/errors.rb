module Mongo
  # Base error class for all Mongo related errors.
  class MongoError < StandardError; end

  # Base error class for all errors coming from the driver.
  class DriverError < MongoError; end

  # Base error class for all errors coming from the server.
  class OperationError < MongoError; end

  class SocketError < StandardError; end
  class SocketTimeoutError < SocketError; end
  class ConnectionError < StandardError; end
end
