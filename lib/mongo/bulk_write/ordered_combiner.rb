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
  class BulkWrite

    # Combines groups of bulk write operations in order.
    #
    # @api private
    #
    # @since 2.1.0
    class OrderedCombiner

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

      # The update many model constant.
      #
      # @since 2.1.0
      UPDATE_MANY = :update_many.freeze

      # The update one model constant.
      #
      # @since 2.1.0
      UPDATE_ONE = :update_one.freeze

      # Document mappers from the bulk api input into proper commands.
      #
      # @since 2.1.0
      MAPPERS = {
        DELETE_MANY => ->(doc){{ q: doc[:filter], limit: 0 }},
        DELETE_ONE  => ->(doc){{ q: doc[:filter], limit: 1 }},
        INSERT_ONE  => ->(doc){ doc },
        UPDATE_MANY  => ->(doc){
          { q: doc[:filter], u: doc[:update], multi: true, upsert: doc.fetch(:upsert, false) }
        },
        UPDATE_ONE  => ->(doc){
          { q: doc[:filter], u: doc[:update], multi: false, upsert: doc.fetch(:upsert, false) }
        }
      }.freeze

      # @return [ Array<Hash, BSON::Document> ] requests The provided requests.
      attr_reader :requests

      # Create the ordered combiner.
      #
      # @api private
      #
      # @example Create the ordered combiner.
      #   OrderedCombiner.new([{ insert_one: { _id: 0 }}])
      #
      # @param [ Array<Hash, BSON::Document> ] requests The bulk requests.
      #
      # @since 2.1.0
      def initialize(requests)
        @requests = requests
      end

      # Combine the requests in order.
      #
      # @api private
      #
      # @example Combine the requests.
      #   combiner.combine
      #
      # @return [ Array<Hash> ] The combined requests.
      #
      # @since 2.1.0
      def combine
        requests.reduce([]) do |operations, request|
          add(operations, request.keys.first, request.values.first)
        end
      end

      private

      def add(operations, name, document)
        operations.push({ name => []}) if next_group?(name, operations)
        operations[-1][name].push(transform(name, document))
        operations
      end

      def next_group?(name, operations)
        !operations[-1] || !operations[-1].key?(name)
      end

      def transform(name, document)
        # VALIDATORS[name].call(name, document)
        validate(name, document)
        MAPPERS[name].call(document)
      end

      def validate(name, document)
        if document.respond_to?(:keys)
          document
        else
          raise Error::InvalidBulkOperation.new(name, document)
        end
      end
    end
  end
end
