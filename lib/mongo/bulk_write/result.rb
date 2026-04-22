# frozen_string_literal: true

# Copyright (C) 2015-2020 MongoDB Inc.
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
    # Wraps a series of bulk write operations in a result object.
    #
    # @since 2.0.6
    class Result
      # @return [ Boolean ] Is the result acknowledged?
      def acknowledged?
        @acknowledged
      end

      # @return [ Array<String> ] Deduplicated list of "host:port" addresses of
      #   the servers that produced this bulk write's operations.
      attr_reader :server_addresses

      # Constant for number removed.
      #
      # @since 2.1.0
      REMOVED_COUNT = 'n_removed'

      # Constant for number inserted.
      #
      # @since 2.1.0
      INSERTED_COUNT = 'n_inserted'

      # Constant for inserted ids.
      #
      # @since 2.1.0
      INSERTED_IDS = 'inserted_ids'

      # Constant for number matched.
      #
      # @since 2.1.0
      MATCHED_COUNT = 'n_matched'

      # Constant for number modified.
      #
      # @since 2.1.0
      MODIFIED_COUNT = 'n_modified'

      # Constant for upserted.
      #
      # @since 2.1.0
      UPSERTED = 'upserted'

      # Constant for number upserted.
      #
      # @since 2.1.0
      UPSERTED_COUNT = 'n_upserted'

      # Constant for upserted ids.
      #
      # @since 2.1.0
      UPSERTED_IDS = 'upserted_ids'

      # The fields contained in the result document returned from executing the
      # operations.
      #
      # @since 2.1.0.
      FIELDS = [
        INSERTED_COUNT,
        REMOVED_COUNT,
        MODIFIED_COUNT,
        UPSERTED_COUNT,
        MATCHED_COUNT,
        Operation::Result::N
      ].freeze

      # Returns the number of documents deleted.
      #
      # @example Get the number of deleted documents.
      #   result.deleted_count
      #
      # @return [ Integer ] The number deleted.
      #
      # @since 2.1.0
      def deleted_count
        @results[REMOVED_COUNT]
      end

      # Create the new result object from the results document.
      #
      # @example Create the new result.
      #   Result.new({ 'n_inserted' => 10 })
      #
      # @param [ BSON::Document, Hash ] results The results document.
      # @param [ Boolean ] acknowledged Is the result acknowledged?
      # @param [ Array<String> ] server_addresses Deduplicated "host:port"
      #   addresses of the servers that produced the underlying operation
      #   results.
      #
      # @since 2.1.0
      #
      # @api private
      def initialize(results, acknowledged, server_addresses = [])
        @results = results
        @acknowledged = acknowledged
        @server_addresses = server_addresses
      end

      # Returns the number of documents inserted.
      #
      # @example Get the number of inserted documents.
      #   result.inserted_count
      #
      # @return [ Integer ] The number inserted.
      #
      # @since 2.1.0
      def inserted_count
        @results[INSERTED_COUNT]
      end

      # Get the inserted document ids, if the operation has inserts.
      #
      # @example Get the inserted ids.
      #   result.inserted_ids
      #
      # @return [ Array<BSON::ObjectId> ] The inserted ids.
      #
      # @since 2.1.0
      def inserted_ids
        @results[INSERTED_IDS]
      end

      # Returns the number of documents matched.
      #
      # @example Get the number of matched documents.
      #   result.matched_count
      #
      # @return [ Integer ] The number matched.
      #
      # @since 2.1.0
      def matched_count
        @results[MATCHED_COUNT]
      end

      # Returns the number of documents modified.
      #
      # @example Get the number of modified documents.
      #   result.modified_count
      #
      # @return [ Integer ] The number modified.
      #
      # @since 2.1.0
      def modified_count
        @results[MODIFIED_COUNT]
      end

      # Returns the number of documents upserted.
      #
      # @example Get the number of upserted documents.
      #   result.upserted_count
      #
      # @return [ Integer ] The number upserted.
      #
      # @since 2.1.0
      def upserted_count
        @results[UPSERTED_COUNT]
      end

      # Get the upserted document ids, if the operation has inserts.
      #
      # @example Get the upserted ids.
      #   result.upserted_ids
      #
      # @return [ Array<BSON::ObjectId> ] The upserted ids.
      #
      # @since 2.1.0
      def upserted_ids
        @results[UPSERTED_IDS] || []
      end

      # Validates the bulk write result.
      #
      # @example Validate the result.
      #   result.validate!
      #
      # @raise [ Error::BulkWriteError ] If the result contains errors.
      #
      # @return [ Result ] The result.
      #
      # @since 2.1.0
      def validate!
        raise Error::BulkWriteError.new(@results, server_addresses: @server_addresses) if @results['writeErrors'] || @results['writeConcernErrors']

        self
      end
    end
  end
end
