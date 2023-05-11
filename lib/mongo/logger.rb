# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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
  # @since 2.0.0
  class Logger

    class << self

      # Get the wrapped logger. If none was set will return a default info
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

      # Get the global logger level.
      #
      # @example Get the global logging level.
      #   Mongo::Logger.level
      #
      # @return [ Integer ] The log level.
      #
      # @since 2.0.0
      def level
        logger.level
      end

      # Set the global logger level.
      #
      # @example Set the global logging level.
      #   Mongo::Logger.level == Logger::DEBUG
      #
      # @return [ Integer ] The log level.
      #
      # @since 2.0.0
      def level=(level)
        logger.level = level
      end

      private

      def default_logger
        logger = ::Logger.new(STDOUT)
        logger.level = ::Logger::INFO
        logger
      end
    end
  end
end
