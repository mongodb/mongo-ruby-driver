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
  module BulkWrite
    module BulkWritable
      extend Forwardable

      def_delegators :@collection, :database, :cluster, :next_primary

      def initialize(collection, operations, options)
        @collection = collection
        @operations = operations
        @options = options
      end

      private

      def insert_one(op)
        Operation::Write::BulkInsert.new(
          # todo flatten necessary?
          :documents => [ op[:insert_one] ].flatten,
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(next_primary.context)
      end

      def delete(ops, limit)
        deletes = ops.collect do |d|
          { q: d, limit: limit }
        end
        Operation::Write::BulkDelete.new(
          :deletes => deletes,
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(next_primary.context)
      end

      def delete_one(op)
        delete(op[:delete_one], 1)
      end

      def delete_many(op)
        delete(op[:delete_many], 0)
      end

      def update(ops, multi)
        updates = ops.collect do |u|
          { q: u[:find],
            u: u[:update],
            multi: multi,
            upsert: u[:upsert]
          }
        end
        Operation::Write::BulkUpdate.new(
          :updates => updates,
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(next_primary.context)
      end

      def update_one(op)
        update(op[:update_one], false)
      end

      def update_many(op)
        update(op[:update_many], true)
      end

      def replace_one(op)
        updates = op[:replace_one].collect do |r|
          { q: r[:find],
            u: r[:replacement],
            multi: false,
            upsert: r[:upsert]
          }
        end
        Operation::Write::BulkUpdate.new(
          :updates => updates,
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(next_primary.context)
      end

      def merge_consecutive_ops(ops)
        ops.inject([]) do |merged, op|
          type = op.keys.first
          previous = merged.last
          if previous && previous.keys.first == type
            merged[-1].merge(type => previous[type] << op[type])
            merged
          else
            merged << { type => [ op[type] ].flatten }
          end
        end
      end

      def merge_ops_by_type
        [ @operations.inject({}) do |merged, op|
          type = op.keys.first
          merged.merge!(type => merged.fetch(type, []).push(op[type]))
        end ]
      end

      def valid_batch_sizes(op, server)
        type = op.keys.first
        ops = []
        until op[type].size < server.max_write_batch_size
          ops << { type => op[type].slice!(0, server.max_write_batch_size) }
        end
        ops << op
      end

      def write_concern
        @write_concern ||= WriteConcern.get(@options[:write_concern]) ||
                            @collection.write_concern
      end

      def process(result)
        @results ||= {}
        write_errors = result.aggregate_write_errors

        @results.merge!(
          'nInserted' => (@results['nInserted'] || 0) + result.n_inserted
        ) if result.respond_to?(:n_inserted)

        @results.merge!(
          'nRemoved' => (@results['nRemoved'] || 0) + result.n_removed
        ) if result.respond_to?(:n_removed)

        @results.merge!(
          'nMatched' => (@results['nMatched'] || 0) + result.n_matched
        ) if result.respond_to?(:n_matched)

        @results.merge!(
          'nModified' => (@results['nModified'] || 0) + result.n_modified
        ) if result.respond_to?(:n_modified)

        @results.merge!(
          'nUpserted' => (@results['nUpserted'] || 0) + result.n_upserted
        ) if result.respond_to?(:n_upserted)

        @results.merge!(
          'writeErrors' => ((@results['writeErrors'] || []) << write_errors).flatten
        ) if write_errors
        @results
      end
    end
  end
end