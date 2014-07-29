module Mongo

  # Base error class for all Mongo related errors.
  #
  # @since 2.0.0
  class MongoError < StandardError; end

  # Base error class for all errors coming from the driver.
  #
  # @since 2.0.0
  class DriverError < MongoError; end

  # Base error class for all errors coming from the server.
  #
  # @since 2.0.0
  class OperationError < MongoError; end

  # MongoDB Core Server error codes, from src/mongo/base/error_codes.err
  #
  # @since 2.0.0
  module ErrorCode

    # A list of possible COMMAND_NOT_FOUND error codes.
    #
    # @since 2.0.0
    COMMAND_NOT_FOUND = [nil, 59, 13390]
  end
end
