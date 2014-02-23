# Copyright (C) 2009-2014 MongoDB, Inc.
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
  # @since 3.0.0
  module Loggable

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
    # @since 3.0.0
    def log(level, prefix, operations)
      started = Time.new
      begin
        yield if block_given?
      rescue Exception => e
        raise e
      ensure
        runtime = ("%.4fms" % (1000 * (Time.now.to_f - started.to_f)))
        operations.each do |operation|
          Logger.send(level, prefix, log_inspect(operation), runtime)
        end
      end
    end

    private

    def log_inspect(operation)
      operation.respond_to?(:log_message) ? operation.log_message : operation
    end
  end
end
