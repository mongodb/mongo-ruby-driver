# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2021 MongoDB Inc.
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
  module Operation

    # @api private
    module Validatable

      def validate_find_options(connection, selector)
        if selector.key?(:hint) &&
          !connection.features.find_and_modify_option_validation_enabled?
        then
          raise Error::UnsupportedOption.hint_error
        end

        if selector.key?(:arrayFilters) &&
          !connection.features.array_filters_enabled?
        then
          raise Error::UnsupportedArrayFilters
        end

        if selector.key?(:collation) &&
          !connection.features.collation_enabled?
        then
          raise Error::UnsupportedCollation
        end
      end

      # selector_or_item here is either:
      # - The selector as used in a findAndModify command, or
      # - One of the array elements in the updates array in an update command.
      def validate_hint_on_update(connection, selector_or_item)
        if selector_or_item.key?(:hint) &&
          !connection.features.update_delete_option_validation_enabled?
        then
          raise Error::UnsupportedOption.hint_error
        end
      end

      # selector_or_item here is either:
      # - The selector as used in a findAndModify command, or
      # - One of the array elements in the updates array in an update command.
      def validate_array_filters(connection, selector_or_item)
        if selector_or_item.key?(:arrayFilters) &&
          !connection.features.array_filters_enabled?
        then
          raise Error::UnsupportedArrayFilters
        end
      end

      # selector_or_item here is either:
      # - The selector as used in a findAndModify command, or
      # - One of the array elements in the updates array in an update command.
      def validate_collation(connection, selector_or_item)
        if selector_or_item.key?(:collation) &&
          !connection.features.collation_enabled?
        then
          raise Error::UnsupportedCollation
        end
      end

      def validate_updates(connection, updates)
        updates.each do |update|
          validate_array_filters(connection, update)
          validate_collation(connection, update)
          validate_hint_on_update(connection, update)
        end
        updates
      end
    end
  end
end
