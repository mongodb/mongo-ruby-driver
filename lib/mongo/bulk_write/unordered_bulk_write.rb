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

    class UnorderedBulkWrite

      include BulkWritable

      def execute
        check_operations!
        merged_ops.each do |op|
          execute_op(op)
        end
        validate!
      end

      private

      def merged_ops
        merge_consecutive_ops(merge_ops_by_type)
      end

      def execute_op(operation)
        server = next_primary
        type = operation.keys.first
        valid_batch_sizes(operation, server).each do |op|
          check_type!(type, op)
          process(send(type, op))
        end
      end

      def ordered?
        false
      end

      def validate!
        raise Error::BulkWriteError.new(@results) if @results['writeErrors']
        @results
      end
    end
  end
end