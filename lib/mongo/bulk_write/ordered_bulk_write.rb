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

    class OrderedBulkWrite

      include BulkWritable

      private

      def ordered?
        true
      end

      def merged_ops
        merge_consecutive_ops(@operations)
      end

      def process(result, indexes)
        combine_results(result, indexes)
        raise Error::BulkWriteError.new(@results) if stop?
      end

      def stop?
        @results.keys.include?(:write_errors)
      end

      def finalize
        raise Error::BulkWriteError.new(@results) if @results[:write_concern_errors]
        @results
      end
    end
  end
end