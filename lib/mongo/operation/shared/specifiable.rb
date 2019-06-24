# Copyright (C) 2014-2019 MongoDB, Inc.
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

    # This module contains common functionality for convenience methods getting
    # various values from the spec.
    #
    # @since 2.0.0
    module Specifiable

      # The field for database name.
      #
      # @since 2.0.0
      DB_NAME = :db_name.freeze

      # The field for deletes.
      #
      # @since 2.0.0
      DELETES = :deletes.freeze

      # The field for delete.
      #
      # @since 2.0.0
      DELETE = :delete.freeze

      # The field for documents.
      #
      # @since 2.0.0
      DOCUMENTS = :documents.freeze

      # The field for collection name.
      #
      # @since 2.0.0
      COLL_NAME = :coll_name.freeze

      # The field for cursor count.
      #
      # @since 2.0.0
      CURSOR_COUNT = :cursor_count.freeze

      # The field for cursor id.
      #
      # @since 2.0.0
      CURSOR_ID = :cursor_id.freeze

      # The field for cursor ids.
      #
      # @since 2.0.0
      CURSOR_IDS = :cursor_ids.freeze

      # The field for an index.
      #
      # @since 2.0.0
      INDEX = :index.freeze

      # The field for multiple indexes.
      #
      # @since 2.0.0
      INDEXES = :indexes.freeze

      # The field for index names.
      #
      # @since 2.0.0
      INDEX_NAME = :index_name.freeze

      # The operation id constant.
      #
      # @since 2.1.0
      OPERATION_ID = :operation_id.freeze

      # The field for options.
      #
      # @since 2.0.0
      OPTIONS = :options.freeze

      # The read concern option.
      #
      # @since 2.2.0
      READ_CONCERN = :read_concern.freeze

      # The max time ms option.
      #
      # @since 2.2.5
      MAX_TIME_MS = :max_time_ms.freeze

      # The field for a selector.
      #
      # @since 2.0.0
      SELECTOR = :selector.freeze

      # The field for number to return.
      #
      # @since 2.0.0
      TO_RETURN = :to_return.freeze

      # The field for updates.
      #
      # @since 2.0.0
      UPDATES = :updates.freeze

      # The field for update.
      #
      # @since 2.0.0
      UPDATE = :update.freeze

      # The field name for a user.
      #
      # @since 2.0.0
      USER = :user.freeze

      # The field name for user name.
      #
      # @since 2.0.0
      USER_NAME = :user_name.freeze

      # The field name for a write concern.
      #
      # @since 2.0.0
      WRITE_CONCERN = :write_concern.freeze

      # The field name for the read preference.
      #
      # @since 2.0.0
      READ = :read.freeze

      # Whether to bypass document level validation.
      #
      # @since 2.2.0
      BYPASS_DOC_VALIDATION = :bypass_document_validation.freeze

      # A collation to apply to the operation.
      #
      # @since 2.4.0
      COLLATION = :collation.freeze

      # @return [ Hash ] spec The specification for the operation.
      attr_reader :spec

      # Check equality of two specifiable operations.
      #
      # @example Are the operations equal?
      #   operation == other
      #
      # @param [ Object ] other The other operation.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        return false unless other.is_a?(Specifiable)
        spec == other.spec
      end
      alias_method :eql?, :==

      # Get the cursor count from the spec.
      #
      # @example Get the cursor count.
      #   specifiable.cursor_count
      #
      # @return [ Integer ] The cursor count.
      #
      # @since 2.0.0
      def cursor_count
        spec[CURSOR_COUNT]
      end

      # The name of the database to which the operation should be sent.
      #
      # @example Get the database name.
      #   specifiable.db_name
      #
      # @return [ String ] Database name.
      #
      # @since 2.0.0
      def db_name
        spec[DB_NAME]
      end

      # Get the deletes from the specification.
      #
      # @example Get the deletes.
      #   specifiable.deletes
      #
      # @return [ Array<BSON::Document> ] The deletes.
      #
      # @since 2.0.0
      def deletes
        spec[DELETES]
      end

      # Get the delete document from the specification.
      #
      # @example Get the delete document.
      #   specifiable.delete
      #
      # @return [ Hash ] The delete document.
      #
      # @since 2.0.0
      def delete
        spec[DELETE]
      end

      # The documents to in the specification.
      #
      # @example Get the documents.
      #   specifiable.documents
      #
      # @return [ Array<BSON::Document> ] The documents.
      #
      # @since 2.0.0
      def documents
        spec[DOCUMENTS]
      end

      # The name of the collection to which the operation should be sent.
      #
      # @example Get the collection name.
      #   specifiable.coll_name
      #
      # @return [ String ] Collection name.
      #
      # @since 2.0.0
      def coll_name
        spec[COLL_NAME]
      end

      # The id of the cursor created on the server.
      #
      # @example Get the cursor id.
      #   specifiable.cursor_id
      #
      # @return [ Integer ] The cursor id.
      #
      # @since 2.0.0
      def cursor_id
        spec[CURSOR_ID]
      end

      # The ids of the cursors to kill from the spec.
      #
      # @example Get the cursor ids from the spec.
      #   specifiable.cursor_ids
      #
      # @return [ Array<Integer> ] The cursor ids.
      #
      # @since 2.0.0
      def cursor_ids
        spec[CURSOR_IDS]
      end

      # Get the index from the specification.
      #
      # @example Get the index specification.
      #   specifiable.index
      #
      # @return [ Hash ] The index specification.
      #
      # @since 2.0.0
      def index
        spec[INDEX]
      end

      # Get the index name from the spec.
      #
      # @example Get the index name.
      #   specifiable.index_name
      #
      # @return [ String ] The index name.
      #
      # @since 2.0.0
      def index_name
        spec[INDEX_NAME]
      end

      # Get the indexes from the specification.
      #
      # @example Get the index specifications.
      #   specifiable.indexes
      #
      # @return [ Hash ] The index specifications.
      #
      # @since 2.0.0
      def indexes
        spec[INDEXES]
      end

      # Create the new specifiable operation.
      #
      # @example Create the new specifiable operation.
      #   Specifiable.new(spec)
      #
      # @param [ Hash ] spec The operation specification.
      #
      # @see The individual operations for the values they require in their
      #   specs.
      #
      # @since 2.0.0
      def initialize(spec)
        @spec = spec
      end

      # Get the operation id for the operation. Used for linking operations in
      # monitoring.
      #
      # @example Get the operation id.
      #   specifiable.operation_id
      #
      # @return [ Integer ] The operation id.
      #
      # @since 2.1.0
      def operation_id
        spec[OPERATION_ID]
      end

      # Get the options for the operation.
      #
      # @example Get the options.
      #   specifiable.options
      #
      # @return [ Hash ] The options.
      #
      # @since 2.0.0
      def options(server = nil)
        spec[OPTIONS] || {}
      end

      # Get the read concern document from the spec.
      #
      # @note The document may include afterClusterTime.
      #
      # @example Get the read concern.
      #   specifiable.read_concern
      #
      # @return [ Hash ] The read concern document.
      #
      # @since 2.2.0
      def read_concern
        spec[READ_CONCERN]
      end

      # Get the max time ms value from the spec.
      #
      # @example Get the max time ms.
      #   specifiable.max_time_ms
      #
      # @return [ Hash ] The max time ms value.
      #
      # @since 2.2.5
      def max_time_ms
        spec[MAX_TIME_MS]
      end

      # Whether or not to bypass document level validation.
      #
      # @example Get the bypass_document_validation option.
      #   specifiable.bypass_documentation_validation.
      #
      # @return [ true, false ] Whether to bypass document level validation.
      #
      # @since 2.2.0
      def bypass_document_validation
        spec[BYPASS_DOC_VALIDATION]
      end

      # The collation to apply to the operation.
      #
      # @example Get the collation option.
      #   specifiable.collation.
      #
      # @return [ Hash ] The collation document.
      #
      # @since 2.4.0
      def collation
        send(self.class::IDENTIFIER).first[COLLATION]
      end

      # The selector for from the specification.
      #
      # @example Get a selector specification.
      #   specifiable.selector.
      #
      # @return [ Hash ] The selector spec.
      #
      # @since 2.0.0
      def selector(server = nil)
        spec[SELECTOR]
      end

      # The number of documents to request from the server.
      #
      # @example Get the to return value from the spec.
      #   specifiable.to_return
      #
      # @return [ Integer ] The number of documents to return.
      #
      # @since 2.0.0
      def to_return
        spec[TO_RETURN]
      end

      # The update documents from the spec.
      #
      # @example Get the update documents.
      #
      # @return [ Array<BSON::Document> ] The update documents.
      #
      # @since 2.0.0
      def updates
        spec[UPDATES]
      end

      # The update document from the spec.
      #
      # @example Get the update document.
      #
      # @return [ Hash ] The update document.
      #
      # @since 2.0.0
      def update
        spec[UPDATE]
      end

      # The user for user related operations.
      #
      # @example Get the user.
      #   specifiable.user
      #
      # @return [ Auth::User ] The user.
      #
      # @since 2.0.0
      def user
        spec[USER]
      end

      # The user name from the specification.
      #
      # @example Get the user name.
      #   specifiable.user_name
      #
      # @return [ String ] The user name.
      #
      # @since 2.0.
      def user_name
        spec[USER_NAME]
      end

      # The write concern to use for this operation.
      #
      # @example Get the write concern.
      #   specifiable.write_concern
      #
      # @return [ Mongo::WriteConcern ] The write concern.
      #
      # @since 2.0.0
      def write_concern
        @spec[WRITE_CONCERN]
      end

      # The read preference for this operation.
      #
      # @example Get the read preference.
      #   specifiable.read
      #
      # @return [ Mongo::ServerSelector ] The read preference.
      #
      # @since 2.0.0
      def read
        @read ||= ServerSelector.get(spec[READ]) if spec[READ]
      end

      # Whether the operation is ordered.
      #
      # @example Get the ordered value, true is the default.
      #   specifiable.ordered?
      #
      # @return [ true, false ] Whether the operation is ordered.
      #
      # @since 2.1.0
      def ordered?
        !!(@spec.fetch(:ordered, true))
      end

      # The namespace, consisting of the db name and collection name.
      #
      # @example Get the namespace.
      #   specifiable.namespace
      #
      # @return [ String ] The namespace.
      #
      # @since 2.1.0
      def namespace
        "#{db_name}.#{coll_name}"
      end

      # The session to use for the operation.
      #
      # @example Get the session.
      #   specifiable.session
      #
      # @return [ Session ] The session.
      #
      # @since 2.5.0
      def session
        @spec[:session]
      end

      # The transaction number for the operation.
      #
      # @example Get the transaction number.
      #   specifiable.txn_num
      #
      # @return [ Integer ] The transaction number.
      #
      # @since 2.5.0
      def txn_num
        @spec[:txn_num]
      end

      # The command.
      #
      # @example Get the command.
      #   specifiable.command
      #
      # @return [ Hash ] The command.
      #
      # @since 2.5.2
      def command(server = nil)
        selector(server)
      end

      # The array filters.
      #
      # @example Get the array filters.
      #   specifiable.array_filters
      #
      # @return [ Hash ] The array filters.
      #
      # @since 2.5.2
      def array_filters
        selector[Operation::ARRAY_FILTERS] if selector
      end

      # Does the operation have an acknowledged write concern.
      #
      # @example Determine whether the operation has an acknowledged write.
      #   specifiable.array_filters
      #
      # @return [ Boolean ] Whether or not the operation has an acknowledged write concern.
      #
      # @since 2.5.2
      def acknowledged_write?
        write_concern.nil? || write_concern.acknowledged?
      end

      private

      def validate_result(result)
        add_error_labels do
          result.validate!
        end
      end

      def add_error_labels
        yield
      rescue Mongo::Error::SocketError => e
        if session && session.in_transaction? && !session.committing_transaction?
          e.add_label('TransientTransactionError')
        end
        if session && session.committing_transaction?
          e.add_label('UnknownTransactionCommitResult')
        end
        raise e
      rescue Mongo::Error::OperationFailure => e
        if session && session.committing_transaction?
          if e.write_retryable? || e.wtimeout? || (e.write_concern_error? &&
              !Session::UNLABELED_WRITE_CONCERN_CODES.include?(e.write_concern_error_code))
            e.add_label('UnknownTransactionCommitResult')
          end
        end
        raise e
      end
    end
  end
end
