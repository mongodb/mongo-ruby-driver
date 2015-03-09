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
        prepare_operations!.each do |op|
          execute_op(op)
        end
        validate_result!
      end

      private

      def prepare_operations!
        validate_operations!
        merge_consecutive_ops(merge_ops_by_type)
      end

      def execute_op(operation)
        server = next_primary
        type = operation.keys.first
        batched_operation(operation, server).each do |op|
          validate_type!(type, op)
          process(send(type, op, server))
        end
      end

      def ordered?
        false
      end

      def validate_result!
        @results.tap do |results|
          raise Error::BulkWriteError.new(results) if results['writeErrors']
        end
      end
    end
  end
end