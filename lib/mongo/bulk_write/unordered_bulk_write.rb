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

      private

      def ordered?
        false
      end

      def merged_ops
        merge_consecutive_ops(merge_ops_by_type)
      end

      def process(result, indexes)
        combine_results(result, indexes)
      end

      def finalize
        @results.tap do |results|
          if results[:write_errors] || results[:write_concern_errors]
            raise Error::BulkWriteError.new(results)
          end
        end
      end
    end
  end
end