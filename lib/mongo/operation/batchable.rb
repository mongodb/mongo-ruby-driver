# Copyright (C) 2009-2014 MongoDB, Inc.
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

    # This module contains common functionality for splitting an operation
    # into the specified number of children operations.
    # An operation including this module must provide a method called
    # #batch_key. It specifies the key of the spec array element to split.
    #
    # @since 2.0.0
    module Batchable

      # Slices this operation into the specified number of children operations.
      #
      # @params [ Integer ] n_batches The number of children operations to split
      #   this one into.
      #
      # @return [ Array ] An array of children operations.
      #
      # @since 2.0.0
      def batch(n_batches)
        items = spec[batch_key]
        raise Exception, "Cannot batch" unless items.size >= n_batches

        items_per_batch = items.size / n_batches
        batches  = items.each_slice(items_per_batch).to_a

        # #each_slice makes groups containing exactly items_per_batch number of items.
        # You could therefore end up with more groups than n_batches, so put the
        # remaining items in the last group.
        if batches.size > n_batches
          batches[n_batches - 1] << batches.pop(batches.size - n_batches)
          batches[-1].flatten!
        end

        batches.inject([]) do |children, batch|
          spec_copy = spec.dup
          spec_copy[batch_key] = batch
          children << self.class.new(spec_copy)
        end
      end

      # Merge another operation with this one.
      # Requires that the collection and database of the two ops are the same.
      #
      # @params[ Mongo::Operation ] The other operation.
      #
      # @return [ self ] This operation merged with the other one.
      #
      # @since 2.0.0
      def merge!(other)
        # @todo: use specific exception
        raise Exception, "Cannot merge" unless self.class == other.class &&
            coll_name == other.coll_name &&
            db_name == other.db_name
        @spec[batch_key] += other.spec[batch_key]
        self
      end

      # Determine if the batch size exceeds a given maximum.
      #
      # @params[ Integer ] The max batch size.
      #
      # @return [ true, false ] If this operation exceeds the given batch size.
      #
      # @since 2.0.0
      def valid_batch_size?(max)
        spec[batch_key].size < max
      end
    end
  end
end
