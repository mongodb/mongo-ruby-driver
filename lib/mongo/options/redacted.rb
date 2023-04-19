# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
                            :pwd ].freeze

      # The replacement string used in place of the value for sensitive keys.
      #
      # @since 2.1.0
      STRING_REPLACEMENT = '<REDACTED>'.freeze

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

      # Returns a new options object consisting of pairs for which the block returns false.
      #
      # @example Get a new options object with pairs for which the block returns false.
      #   new_options = options.reject { |k, v| k == 'database' }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted ] A new options object.
      #
      # @since 2.1.0
      def reject(&block)
        new_options = dup
        new_options.reject!(&block) || new_options
      end

      # Only keeps pairs for which the block returns false.
      #
      # @example Remove pairs from this object for which the block returns true.
      #   options.reject! { |k, v| k == 'database' }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted, nil ] This object or nil if no changes were made.
      #
      # @since 2.1.0
      def reject!
        if block_given?
          n_keys = keys.size
          keys.each do |key|
            delete(key) if yield(key, self[key])
          end
          n_keys == keys.size ? nil : self
        else
          to_enum
        end
      end

      # Returns a new options object consisting of pairs for which the block returns true.
      #
      # @example Get a new options object with pairs for which the block returns true.
      #   ssl_options = options.select { |k, v| k =~ /ssl/ }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted ] A new options object.
      #
      # @since 2.1.0
      def select(&block)
        new_options = dup
        new_options.select!(&block) || new_options
      end

      # Only keeps pairs for which the block returns true.
      #
      # @example Remove pairs from this object for which the block does not return true.
      #   options.select! { |k, v| k =~ /ssl/ }
      #
      # @yieldparam [ String, Object ] The key as a string and its value.
      #
      # @return [ Options::Redacted, nil ] This object or nil if no changes were made.
      #
      # @since 2.1.0
      def select!
        if block_given?
          n_keys = keys.size
          keys.each do |key|
            delete(key) unless yield(key, self[key])
          end
          n_keys == keys.size ? nil : self
        else
          to_enum
        end
      end

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
