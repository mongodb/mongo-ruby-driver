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

require 'logger'

module Mongo

  # Provides ability to log messages.
  #
  # @since 2.0.0
  class Logger

    class << self

      # Log a debug level message.
      #
      # @example Log a debug level message.
      #   Logger.debug('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] message The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 2.0.0
      def debug(prefix, message, runtime)
        self.log(:debug, prefix, message, runtime)
      end

      # Log a error level message.
      #
      # @example Log a error level message.
      #   Logger.error('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] message The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 2.0.0
      def error(prefix, message, runtime)
        self.log(:error, prefix, message, runtime)
      end

      # Log a fatal level message.
      #
      # @example Log a fatal level message.
      #   Logger.fatal('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] message The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 2.0.0
      def fatal(prefix, message, runtime)
        self.log(:fatal, prefix, message, runtime)
      end

      # Log a info level message.
      #
      # @example Log a info level message.
      #   Logger.info('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] message The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 2.0.0
      def info(prefix, message, runtime)
        self.log(:info, prefix, message, runtime)
      end

      # Log a warn level message.
      #
      # @example Log a warn level message.
      #   Logger.warn('mongo', 'message', '10ms')
      #
      # @param [ String ] prefix The category prefix.
      # @param [ String ] message The log message.
      # @param [ String ] runtime The time of the operation.
      #
      # @since 2.0.0
      def warn(prefix, message, runtime)
        self.log(:warn, prefix, message, runtime)
      end

      # Get the wrapped logger. If none was set will return a default debug
      # level logger.
      #
      # @example Get the wrapped logger.
      #   Mongo::Logger.logger
      #
      # @return [ ::Logger ] The wrapped logger.
      #
      # @since 2.0.0
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
      # @since 2.0.0
      def logger=(other)
        @logger = other
      end

      def log(level, prefix, message, runtime)
        logger.send(level, format("%s | %s | runtime: %s".freeze, prefix, message, runtime))
      end

      def allow?(level)
        logger.send(:"#{level}?")
      end

      def level
        logger.level
      end

      def level=(level)
        logger.level = level
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
