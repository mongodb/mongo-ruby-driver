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
        validate_insert_operations!(op[:insert_one])
        Operation::Write::BulkInsert.new(
          :documents => op[:insert_one].flatten,
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(next_primary.context)
      end

      def delete(ops, limit)
        deletes = ops.collect do |d|
          raise Error::InvalidBulkOperation.new(__method__, d) unless valid_doc?(d)
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
          unless u[:find] && u[:update] && update_doc?(u[:update])
            raise Error::InvalidBulkOperation.new(__method__, u)
          end
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
          unless r[:find] && r[:replacement] && replacement_doc?(r[:replacement])
            raise Error::InvalidBulkOperation.new(__method__, r)
          end
          { q: r[:find],
            u: r[:replacement],
            multi: false,
            upsert: r.fetch(:upsert, false)
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

        [:n_inserted, :n_removed, :n_modified, :n_upserted, :n_matched].each do |count|
          @results.merge!(
          count => (@results[count] || 0) + result.send(count)
        ) if result.respond_to?(count) 
        end

        @results.merge!(
          'writeErrors' => ((@results['writeErrors'] || []) << write_errors).flatten
        ) if write_errors
        @results
      end

      def valid_doc?(doc)
        doc.respond_to?(:keys)
      end

      def replacement_doc?(doc)
        doc.respond_to?(:keys) && doc.keys.all?{|key| key !~ /^\$/}
      end

      def update_doc?(doc)
        !doc.empty? &&
          doc.respond_to?(:keys) &&
          doc.keys.first.to_s =~ /^\$/
      end

      def validate_type!(type, op)
        raise Error::InvalidBulkOperation.new(type, op) unless respond_to?(type, true)
      end

      def validate_operations!
        unless @operations && @operations.size > 0
          raise ArgumentError.new('Operations cannot be empty')
        end
      end

      def validate_insert_operations!(inserts)
        if inserts.empty?
          raise Error::InvalidBulkOperation.new(__method__, inserts)
        end
        inserts.each do |i|
          unless valid_doc?(i)
            raise Error::InvalidBulkOperation.new(__method__, i)
          end
        end
      end
    end
  end
end