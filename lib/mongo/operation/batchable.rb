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
    # #batchable_key. It specifies the key of the spec array element to split.
    #
    # @since 2.0.0
    module Batchable

      # Batches this operation into the specified number of children operations.
      #
      # @params [ Integer ] n_batches The number of children operations to split
      #   this one into.
      #
      # @return [ Array ] An array of children operations.
      #
      # @since 2.0.0
      def batch(n_batches)
        items      = spec[batchable_key]
        group_size = items.size / n_batches
        divisions  = items.each_slice(group_size).to_a

        # #each_slice makes groups containing exactly group_size number of items.
        # You could therefore end up with more groups than n_batches, so put the
        # remaining items in the last group.
        if divisions.size > n_batches
          divisions[n_batches - 1] << divisions.pop(divisions.size - n_batches)
          divisions[-1].flatten!
        end

        divisions.inject([]) do |children, division|
          spec_copy = spec.dup
          spec_copy[batchable_key] = division
          children << self.class.new(spec_copy)
        end
      end

      # Set a field :ord in the spec that keeps track of a higher-level ordering.
      #
      # @param [ Integer ] order The higher-level ordering of this op.
      #
      # @since 2.0.0
      def set_order(order)
        spec[batchable_key].each { |doc| doc[:ord] = order }
      end

      private

      # Whether this batch's execution should be ordered.
      # Ordered means that the server will stop the batch operations upon first error.
      # Ordered also means the driver will stop sending operations to the server upon
      # first error.
      #
      # @return [ true, false ] Whether the batch is ordered.
      #
      # @since 2.0.0
      def ordered?
        options.fetch(:ordered, true)
      end

      # Split up the list of operations at batchable_key into max_write_batch_size
      # batches.
      # maxWriteBatchSize is a value defined by the server that limits the number of
      # writes in a write command.
      #
      # @param [ Mongo::Server::Context ] context The context for these ops.
      #
      # @since 2.0.0
      def batches(context)
        spec[batchable_key].each_slice(context.max_write_batch_size).to_a
      end
    end
  end
end
