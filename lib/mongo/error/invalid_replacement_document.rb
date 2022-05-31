# frozen_string_literal: true
# encoding: utf-8

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

    # Exception raised if the object is not a valid replacement document.
    class InvalidReplacementDocument < Error

      # The error message.
      #
      # @deprecated
      MESSAGE = 'Invalid replacement document provided'.freeze

      # Construct the error message.
      #
      # @param [ String ] key The invalid key.
      #
      # @return [ String ] The error message.
      #
      # @api private
      def self.message(key)
        message = "Invalid replacement document provided. Replacement documents "
        message += "must not contain atomic modifiers. The \"#{key}\" key is invalid."
        message
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
