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

  # Provides ability to log messages.
  #
  # @since 3.0.0
  class Logger

    class << self

      # Log a debug level message.
      #
      # @example Log a debug level message.
      #   Logger.debug('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] payload The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 3.0.0
      def debug(prefix, payload, runtime)
        logger.debug("#{prefix} | #{payload} | runtime: #{runtime}")
      end

      # Log a error level message.
      #
      # @example Log a error level message.
      #   Logger.error('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] payload The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 3.0.0
      def error(prefix, payload, runtime)
        logger.error("#{prefix} | #{payload} | runtime: #{runtime}")
      end

      # Log a fatal level message.
      #
      # @example Log a fatal level message.
      #   Logger.fatal('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] payload The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 3.0.0
      def fatal(prefix, payload, runtime)
        logger.fatal("#{prefix} | #{payload} | runtime: #{runtime}")
      end

      # Log a info level message.
      #
      # @example Log a info level message.
      #   Logger.info('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] payload The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 3.0.0
      def info(prefix, payload, runtime)
        logger.info("#{prefix} | #{payload} | runtime: #{runtime}")
      end

      # Log a warn level message.
      #
      # @example Log a warn level message.
      #   Logger.warn('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] payload The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 3.0.0
      def warn(prefix, payload, runtime)
        logger.warn("#{prefix} | #{payload} | runtime: #{runtime}")
      end

      # Get the wrapped logger. If none was set will return a default debug
      # level logger.
      #
      # @example Get the wrapped logger.
      #   Mongo::Logger.logger
      #
      # @return [ ::Logger ] The wrapped logger.
      #
      # @since 3.0.0
      def logger
        @logger ||= default_logger
      end

      # Set the logger.
      #
      # @example Set the wrapped logger.
      #   Mongo::Logger.logger = logger
      #
      # @param [ ::Logger ] other The logger to set.
      #
      # @return [ ::Logger ] The wrapped logger.
      #
      # @since 3.0.0
      def logger=(other)
        @logger = other
      end

      private

      def default_logger
        logger = ::Logger.new($stdout)
        logger.level = ::Logger::DEBUG
        logger
      end
    end
  end
end
