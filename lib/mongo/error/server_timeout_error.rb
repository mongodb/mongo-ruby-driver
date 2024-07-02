# frozen_string_literal: true

require 'mongo/error/timeout_error'

module Mongo
  class Error
    # Raised when the server returns error code 50.
    class ServerTimeoutError < TimeoutError
      include OperationFailure::Family
    end
  end
end
