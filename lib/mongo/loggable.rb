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

    # Convenience method to log debug messages with the standard prefix.
    #
    # @example Log a debug message.
    #   log_debug('Message')
    #
    # @param [ String ] message The message to log.
    #
    # @since 2.0.0
    def log_debug(message)
      logger.debug(format_message(message)) if logger.debug?
    end

    # Convenience method to log error messages with the standard prefix.
    #
    # @example Log a error message.
    #   log_error('Message')
    #
    # @param [ String ] message The message to log.
    #
    # @since 2.0.0
    def log_error(message)
      logger.error(format_message(message)) if logger.error?
    end

    # Convenience method to log fatal messages with the standard prefix.
    #
    # @example Log a fatal message.
    #   log_fatal('Message')
    #
    # @param [ String ] message The message to log.
    #
    # @since 2.0.0
    def log_fatal(message)
      logger.fatal(format_message(message)) if logger.fatal?
    end

    # Convenience method to log info messages with the standard prefix.
    #
    # @example Log a info message.
    #   log_info('Message')
    #
    # @param [ String ] message The message to log.
    #
    # @since 2.0.0
    def log_info(message)
      logger.info(format_message(message)) if logger.info?
    end

    # Convenience method to log warn messages with the standard prefix.
    #
    # @example Log a warn message.
    #   log_warn('Message')
    #
    # @param [ String ] message The message to log.
    #
    # @since 2.0.0
    def log_warn(message)
      logger.warn(format_message(message)) if logger.warn?
    end

    # Get the logger instance.
    #
    # @example Get the logger instance.
    #   loggable.logger
    #
    # @return [ Logger ] The logger.
    #
    # @since 2.1.0
    def logger
      ((options && options[:logger]) || Logger.logger)
    end

    private

    def format_message(message)
      format("%s | %s".freeze, PREFIX, message)
    end
  end
end
