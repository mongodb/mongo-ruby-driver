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
  class BulkWrite
    extend Forwardable

    def_delegators :@collection, :database, :cluster, :next_primary

    def initialize(collection, operations, options)
      @collection = collection
      @operations = operations
      @options = options
    end

    def execute
      merged_ops.each do |op|
        execute_op(op)
      end
      @results
    end

    private

    def merged_ops
      if ordered?
        merge_consecutive_ops(@operations)
      else
        merge_ops_by_type(@operations)
      end
    end

    def merge_consecutive_ops(ops)
      ops.inject([]) do |merged, op|
        type = op.keys.first
        previous_op = merged.last
        if previous_op && previous_op.keys.first == type
          merged[-1] = { type => ([ previous_op[type] ] << op[type]).flatten }
          merged
        else
          merged << op
        end
      end
    end
 
    def merge_ops_by_type(ops)
      ops_by_type = ops.inject({}) do |merged, op|
        merged[op.class] = merged.fetch(op.class, []).push(op)
      end
 
      ops_by_type.keys.inject([]) do |merged, type|
        merged << merge_consecutive_ops( ops_by_type[ type ] )
      end.flatten
    end

    def execute_op(operation)
      server = next_primary
      type = operation.keys.first
      valid_batch_sizes(operation, server).each do |op|
        validate!(send(type, op))
      end
    end

    def valid_batch_sizes(op, server)
      type = op.keys.first
      ops = []
      until op[type].size < server.max_write_batch_size
        ops << { type => op[type].slice!(0, server.max_write_batch_size) }
      end
      ops << op
    end

    def insert_one(op)
      Operation::Write::BulkInsert.new(
        :documents => [ op[:insert_one] ].flatten,
        :db_name => database.name,
        :coll_name => @collection.name,
        :write_concern => write_concern,
        :ordered => ordered?
      ).execute(next_primary.context)
    end

    def validate!(result)
      process(result)
      if ordered? && !result.successful?
        raise Error::BulkWriteError.new(@results)
      end
    end

    def ordered?
      @ordered ||= @options.fetch(:ordered, true)
    end

    def write_concern
      @write_concern ||= WriteConcern.get(@options[:write_concern]) ||
                          @collection.write_concern
    end

    def process(result)
      @results ||= {}
      @results.merge!({
        'nInserted' => (@results['nInserted'] || 0) + result.n_inserted
      })
    end
  end
end