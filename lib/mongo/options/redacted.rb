# Copyright (C) 2015 MongoDB, Inc.
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
  module Options

    # Class for wrapping options that could be sensitive.
    # When printed, the sensitive values will be redacted.
    #
    # @since 2.1.0
    class Redacted < BSON::Document

      # The options whose values will be redacted.
      #
      # @since 2.1.0
      SENSITIVE_OPTIONS = [ :password,
                            :pwd ]

      # The replacement string used in place of the value for sensitive keys.
      #
      # @since 2.1.0
      STRING_REPLACEMENT = '<REDACTED>'

      # Get a string representation of the options.
      #
      # @return [ String ] The string representation of the options.
      #
      # @since 2.1.0
      def inspect
        redacted_string(:inspect)
      end

      # Get a string representation of the options.
      #
      # @return [ String ] The string representation of the options.
      #
      # @since 2.1.0
      def to_s
        redacted_string(:to_s)
      end

      # Whether these options contain a given key.
      #
      # @example Determine if the options contain a given key.
      #   options.has_key?(:name)
      #
      # @param [ String, Symbol ] key The key to check for existence.
      #
      # @return [ true, false ] If the options contain the given key.
      #
      # @since 2.1.0
      def has_key?(key)
        super(convert_key(key))
      end
      alias_method :key?, :has_key?

      private

      def redacted_string(method)
        '{' + reduce([]) do |list, (k, v)|
          list << "#{k.send(method)}=>#{redact(k, v, method)}"
        end.join(', ') + '}'
      end

      def redact(k, v, method)
        return STRING_REPLACEMENT if SENSITIVE_OPTIONS.include?(k.to_sym)
        v.send(method)
      end
    end
  end
end
