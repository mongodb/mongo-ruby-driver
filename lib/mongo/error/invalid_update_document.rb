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
  class Error

    # Exception raised if the object is not a valid update document.
    #
    # @since 2.0.0
    class InvalidUpdateDocument < Error

      # The error message.
      #
      # @deprecated
      MESSAGE = 'Invalid update document provided'.freeze

      # Construct the error message.
      #
      # @param [ String ] key The invalid key.
      #
      # @return [ String ] The error message.
      #
      # @api private
      def self.message(key)
        message = "Invalid update document provided. Updates documents must only "
        message += "contain only atomic modifiers. The \"#{key}\" key is invalid."
        message
      end

      # Send and cache the warning.
      #
      # @api private
      def self.warn(logger, key)
        @warned ||= begin
          logger.warn(message(key))
          true
        end
      end

      # Instantiate the new exception.
      #
      # @param [ String ] :key The invalid key.
      def initialize(key: nil)
        super(self.class.message(key))
      end
    end
  end
end
