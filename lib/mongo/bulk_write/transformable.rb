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
  class BulkWrite

    # Defines behavior around transformations.
    #
    # @api private
    #
    # @since 2.1.0
    module Transformable

      # The delete many model constant.
      #
      # @since 2.1.0
      DELETE_MANY = :delete_many.freeze

      # The delete one model constant.
      #
      # @since 2.1.0
      DELETE_ONE = :delete_one.freeze

      # The insert one model constant.
      #
      # @since 2.1.0
      INSERT_ONE = :insert_one.freeze

      # The replace one model constant.
      #
      # @since 2.1.0
      REPLACE_ONE = :replace_one.freeze

      # The update many model constant.
      #
      # @since 2.1.0
      UPDATE_MANY = :update_many.freeze

      # The update one model constant.
      #
      # @since 2.1.0
      UPDATE_ONE = :update_one.freeze

      # Proc to transform delete many ops.
      #
      # @since 2.1.0
      DELETE_MANY_TRANSFORM = ->(doc){
        {
          Operation::Q => doc[:filter],
          Operation::LIMIT => 0,
        }.tap do |d|
          d[Operation::COLLATION] = doc[:collation] if doc[:collation]
          d['hint'] = doc[:hint] if doc[:hint]
        end
      }

      # Proc to transform delete one ops.
      #
      # @since 2.1.0
      DELETE_ONE_TRANSFORM = ->(doc){
        {
          Operation::Q => doc[:filter],
          Operation::LIMIT => 1,
        }.tap do |d|
          d[Operation::COLLATION] = doc[:collation] if doc[:collation]
          d['hint'] = doc[:hint] if doc[:hint]
        end
      }

      # Proc to transform insert one ops.
      #
      # @since 2.1.0
      INSERT_ONE_TRANSFORM = ->(doc){
        doc
      }

      # Proc to transfor replace one ops.
      #
      # @since 2.1.0
      REPLACE_ONE_TRANSFORM = ->(doc){
        {
          Operation::Q => doc[:filter],
          Operation::U => doc[:replacement],
        }.tap do |d|
          d['upsert'] = true if doc[:upsert]
          d[Operation::COLLATION] = doc[:collation] if doc[:collation]
          d['hint'] = doc[:hint] if doc[:hint]
        end
      }

      # Proc to transform update many ops.
      #
      # @since 2.1.0
      UPDATE_MANY_TRANSFORM = ->(doc){
        {
          Operation::Q => doc[:filter],
          Operation::U => doc[:update],
          Operation::MULTI => true,
        }.tap do |d|
          d['upsert'] = true if doc[:upsert]
          d[Operation::COLLATION] = doc[:collation] if doc[:collation]
          d[Operation::ARRAY_FILTERS] = doc[:array_filters] if doc[:array_filters]
          d['hint'] = doc[:hint] if doc[:hint]
        end
      }

      # Proc to transform update one ops.
      #
      # @since 2.1.0
      UPDATE_ONE_TRANSFORM = ->(doc){
        {
          Operation::Q => doc[:filter],
          Operation::U => doc[:update],
        }.tap do |d|
          d['upsert'] = true if doc[:upsert]
          d[Operation::COLLATION] = doc[:collation] if doc[:collation]
          d[Operation::ARRAY_FILTERS] = doc[:array_filters] if doc[:array_filters]
          d['hint'] = doc[:hint] if doc[:hint]
        end
      }

      # Document mappers from the bulk api input into proper commands.
      #
      # @since 2.1.0
      MAPPERS = {
        DELETE_MANY => DELETE_MANY_TRANSFORM,
        DELETE_ONE  => DELETE_ONE_TRANSFORM,
        INSERT_ONE  => INSERT_ONE_TRANSFORM,
        REPLACE_ONE => REPLACE_ONE_TRANSFORM,
        UPDATE_MANY => UPDATE_MANY_TRANSFORM,
        UPDATE_ONE  => UPDATE_ONE_TRANSFORM
      }.freeze

      private

      def transform(name, document)
        validate(name, document)
        MAPPERS[name].call(document)
      end
    end
  end
end
