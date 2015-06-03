# Copyright (C) 2014-2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo

  # Allows objects to easily log operations.
  #
  # @since 2.0.0
  module Loggable

    # The standard MongoDB log prefix.
    #
    # @since 2.0.0
    PREFIX = 'MONGODB'.freeze

    # Log the operations. If a block is provided it will be yielded to,
    # otherwise only the logging will take place.
    #
    # @example Log a query operation.
    #   loggable.log(:debug, "MONGO.query", operations)
    #
    # @param [ Symbol ] level The log level.
    # @param [ String ] prefix The prefix for the log line.
    # @param [ Array<Object> ] operations The operations to log. The must
    #   respond to #log_message.
    #
    # @return [ Object ] The result of the block or nil if no block given.
    #
    # @since 2.0.0
    def log(level, prefix, operations)
      started = Time.new
      begin
        yield(operations) if block_given?
      rescue Exception => e
        raise e
      ensure
        if Logger.allow?(level)
          runtime = format("%.4fms", (Time.now.to_f - started.to_f) * 1000.0)
          operations.each do |operation|
            Logger.log(level, prefix, log_inspect(operation), runtime)
          end
        end
      end
    end

    # Convenience method to log debug messages with the standard prefix.
    #
    # @example Log a debug message.
    #   log_debug([ 'Message' ])
    #
    # @param [ Array<Operation, String> ] operations The operations or messages
    #   to log.
    #
    # @since 2.0.0
    def log_debug(operations, &block)
      log(:debug, PREFIX, operations, &block)
    end

    # Convenience method to log error messages with the standard prefix.
    #
    # @example Log a error message.
    #   log_error([ 'Message' ])
    #
    # @param [ Array<Operation, String> ] operations The operations or messages
    #   to log.
    #
    # @since 2.0.0
    def log_error(operations, &block)
      log(:error, PREFIX, operations, &block)
    end

    # Convenience method to log fatal messages with the standard prefix.
    #
    # @example Log a fatal message.
    #   log_fatal([ 'Message' ])
    #
    # @param [ Array<Operation, String> ] operations The operations or messages
    #   to log.
    #
    # @since 2.0.0
    def log_fatal(operations, &block)
      log(:fatal, PREFIX, operations, &block)
    end

    # Convenience method to log info messages with the standard prefix.
    #
    # @example Log a info message.
    #   log_info([ 'Message' ])
    #
    # @param [ Array<Operation, String> ] operations The operations or messages
    #   to log.
    #
    # @since 2.0.0
    def log_info(operations, &block)
      log(:info, PREFIX, operations, &block)
    end

    # Convenience method to log warn messages with the standard prefix.
    #
    # @example Log a warn message.
    #   log_warn([ 'Message' ])
    #
    # @param [ Array<Operation, String> ] operations The operations or messages
    #   to log.
    #
    # @since 2.0.0
    def log_warn(operations, &block)
      log(:warn, PREFIX, operations, &block)
    end

    private

    def log_inspect(operation)
      operation.respond_to?(:log_message) ? operation.log_message : operation
    end
  end
end
