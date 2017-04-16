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
  module Response

    # A response object for Bulk operations.
    #
    # @since 2.0.0
    class Bulk
      include Responsive

      # Initialize a new Bulk Response object
      #
      # @param [ Array ] responses An array of Response objects.
      #
      # @since 2.0.0
      def initialize(operations)
        @operations = operations
      end

      # Get the 'nInserted' field from a bulk response object.
      #
      # @return [ Integer ] the number of documents inserted.
      #
      # @since 2.0.0
      def n_inserted
        @nInserted ||= tally_ops(:n_inserted)
      end

      # Get the 'nRemoved' field from a bulk response object.
      #
      # @return [ Integer ] the number of documents removed.
      #
      # @since 2.0.0
      def n_removed
        @nRemoved ||= tally_ops(:n_removed)
      end

      # Get the 'nMatched' field from a bulk response object.
      #
      # @return [ Integer ] the number of documents matched.
      #
      # @since 2.0.0
      def n_matched
        @nMatched ||= tally_ops(:n_matched)
      end

      # Get the 'nModified' field from a bulk response object.
      #
      # @return [ Integer ] the number of documents modified.
      #
      # @since 2.0.0
      def n_modified
        @nModified ||= tally_ops(:n_modified)
      end

      # Get the 'nUpserted' field from a bulk response object.
      #
      # @return [ Integer ] the number of documents upserted.
      #
      # @since 2.0.0
      def n_upserted
        @nUpserted ||= tally_ops(:n_upserted)
      end

      # Get the 'upserted' field from a bulk response object.
      #
      # @return [ Array ] an array of upserted _ids and indices.
      #
      # @since 2.0.0
      def upserted
        @upserted ||= tally_upserted
      end

      # Get the 'ok' field from a bulk response object.
      #
      # @return [ Integer ] the 'ok' value.
      #
      # @since 2.0.0
      def ok
        @ok ||= tally_ops(:ok)
      end

      # Get the 'n' field from a bulk response object.
      #
      # @return [ Integer ] the 'n' value.
      #
      # @since 2.0.0
      def n
        @n ||= tally_ops(:n)
      end

      # Return a hash of this bulk response object.
      #
      # @return [ Hash ] a hash representing this response.
      #
      # @since 2.0.0
      def to_hash
        { :ok        => ok,
          :n         => n,
          :nModified => n_modified,
          :nMatched  => n_matched,
          :nRemoved  => n_removed,
          :nInserted => n_inserted,
          :nUpserted => n_upserted,
          :upserted  => upserted }
      end

      private

      # Assemble an array of upserted documents for this bulk response.
      #
      # @return [ Array ] the upserted documents.
      #
      # @since 2.0.0
      def tally_upserted
        @operations.collect_concat do |op|
          op.upserted || {}
        end
      end

      # Parse out some field for this bulk operation.
      #
      # @param [ Symbol ] method The name of the method to call on each operation.
      #
      # @return [ Integer ] the field value.
      #
      # @since 2.0.0
      def tally_ops(method)
        tally = 0
        @operations.each do |op|
          tally += op.send(method) if op.method_defined?(method)
        end
        tally
      end
    end
  end
end
