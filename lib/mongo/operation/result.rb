# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

require 'mongo/operation/shared/result/aggregatable'
require 'mongo/operation/shared/result/use_legacy_error_parser'

module Mongo
  module Operation

    # Result wrapper for wire protocol replies.
    #
    # An operation has zero or one replies. The only operations producing zero
    # replies are unacknowledged writes; all other operations produce one reply.
    # This class provides an object that can be operated on (for example, to
    # check whether an operation succeeded) even when the operation did not
    # produce a reply (in which case it is assumed to have succeeded).
    #
    # @since 2.0.0
    # @api semiprivate
    class Result
      extend Forwardable
      include Enumerable

      # The field name for the cursor document in an aggregation.
      #
      # @since 2.2.0
      # @api private
      CURSOR = 'cursor'.freeze

      # The cursor id field in the cursor document.
      #
      # @since 2.2.0
      # @api private
      CURSOR_ID = 'id'.freeze

      # The field name for the first batch of a cursor.
      #
      # @since 2.2.0
      # @api private
      FIRST_BATCH = 'firstBatch'.freeze

      # The field name for the next batch of a cursor.
      #
      # @since 2.2.0
      # @api private
      NEXT_BATCH = 'nextBatch'.freeze

      # The namespace field in the cursor document.
      #
      # @since 2.2.0
      # @api private
      NAMESPACE = 'ns'.freeze

      # The number of documents updated in the write.
      #
      # @since 2.0.0
      # @api private
      N = 'n'.freeze

      # The ok status field in the result.
      #
      # @since 2.0.0
      # @api private
      OK = 'ok'.freeze

      # The result field constant.
      #
      # @since 2.2.0
      # @api private
      RESULT = 'result'.freeze

      # Initialize a new result.
      #
      # For an unkacknowledged write, pass nil in replies.
      #
      # For all other operations, replies must be a Protocol::Message instance
      # or an array containing a single Protocol::Message instance.
      #
      # @param [ Protocol::Message | Array<Protocol::Message> | nil ] replies
      #  The wire protocol replies.
      # @param [ Server::Description | nil ] connection_description
      #   Server description of the server that performed the operation that
      #   this result is for. This parameter is allowed to be nil for
      #   compatibility with existing mongo_kerberos library, but should
      #   always be not nil in the driver proper.
      # @param [ Integer ] connection_global_id
      #   Global id of the connection on which the operation that
      #   this result is for was performed.
      #
      # @api private
      def initialize(replies, connection_description = nil, connection_global_id = nil)
        if replies
          if replies.is_a?(Array)
            if replies.length != 1
              raise ArgumentError, "Only one (or zero) reply is supported, given #{replies.length}"
            end
            reply = replies.first
          else
            reply = replies
          end
          unless reply.is_a?(Protocol::Message)
            raise ArgumentError, "Argument must be a Message instance, but is a #{reply.class}: #{reply.inspect}"
          end
          @replies = [ reply ]
          @connection_description = connection_description
          @connection_global_id = connection_global_id
        end
      end

      # @return [ Array<Protocol::Message> ] replies The wrapped wire protocol replies.
      #
      # @api private
      attr_reader :replies

      # @return [ Server::Description ] Server description of the server that
      #   the operation was performed on that this result is for.
      #
      # @api private
      attr_reader :connection_description

      # @return [ Object ] Global is of the connection that
      #   the operation was performed on that this result is for.
      #
      # @api private
      attr_reader :connection_global_id

      # @api private
      def_delegators :parser,
        :not_master?, :node_recovering?, :node_shutting_down?

      # Is the result acknowledged?
      #
      # @note On MongoDB 2.6 and higher all writes are acknowledged since the
      #   driver uses write commands for all write operations. On 2.4 and
      #   lower, the result is acknowledged if the GLE has been executed after
      #   the command. If not, no replies will be specified. Reads will always
      #   return true here since a replies is always provided.
      #
      # @return [ true, false ] If the result is acknowledged.
      #
      # @since 2.0.0
      # @api public
      def acknowledged?
        !!@replies
      end

      # Whether the result contains cursor_id
      #
      # @return [ true, false ] If the result contains cursor_id.
      #
      # @api private
      def has_cursor_id?
        acknowledged? && replies.last.respond_to?(:cursor_id)
      end

      # Get the cursor id if the response is acknowledged.
      #
      # @note Cursor ids of 0 indicate there is no cursor on the server.
      #
      # @example Get the cursor id.
      #   result.cursor_id
      #
      # @return [ Integer ] The cursor id.
      #
      # @since 2.0.0
      # @api private
      def cursor_id
        acknowledged? ? replies.last.cursor_id : 0
      end

      # Get the namespace of the cursor. The method should be defined in
      # result classes where 'ns' is in the server response.
      #
      # @return [ Nil ]
      #
      # @since 2.0.0
      # @api private
      def namespace
        nil
      end

      # Get the documents in the result.
      #
      # @example Get the documents.
      #   result.documents
      #
      # @return [ Array<BSON::Document> ] The documents.
      #
      # @since 2.0.0
      # @api public
      def documents
        if acknowledged?
          replies.flat_map(&:documents)
        else
          []
        end
      end

      # Iterate over the documents in the replies.
      #
      # @example Iterate over the documents.
      #   result.each do |doc|
      #     p doc
      #   end
      #
      # @return [ Enumerator ] The enumerator.
      #
      # @yieldparam [ BSON::Document ] Each document in the result.
      #
      # @since 2.0.0
      # @api public
      def each(&block)
        documents.each(&block)
      end

      # Get the pretty formatted inspection of the result.
      #
      # @example Inspect the result.
      #   result.inspect
      #
      # @return [ String ] The inspection.
      #
      # @since 2.0.0
      # @api public
      def inspect
        "#<#{self.class.name}:0x#{object_id} documents=#{documents}>"
      end

      # Get the reply from the result.
      #
      # Returns nil if there is no reply (i.e. the operation was an
      # unacknowledged write).
      #
      # @return [ Protocol::Message ] The first reply.
      #
      # @since 2.0.0
      # @api private
      def reply
        if acknowledged?
          replies.first
        else
          nil
        end
      end

      # Get the number of documents returned by the server in this batch.
      #
      # @return [ Integer ] The number of documents returned.
      #
      # @since 2.0.0
      # @api public
      def returned_count
        if acknowledged?
          reply.number_returned
        else
          0
        end
      end

      # If the result was a command then determine if it was considered a
      # success.
      #
      # @note If the write was unacknowledged, then this will always return
      #   true.
      #
      # @example Was the command successful?
      #   result.successful?
      #
      # @return [ true, false ] If the command was successful.
      #
      # @since 2.0.0
      # @api public
      def successful?
        return true if !acknowledged?
        if first_document.has_key?(OK)
          ok? && parser.message.empty?
        else
          !query_failure? && parser.message.empty?
        end
      end

      # Check the first document's ok field.
      #
      # @example Check the ok field.
      #   result.ok?
      #
      # @return [ true, false ] If the command returned ok.
      #
      # @since 2.1.0
      # @api public
      def ok?
        # first_document[OK] is a float, and the server can return
        # ok as a BSON int32, BSON int64 or a BSON double.
        # The number 1 is exactly representable in a float, hence
        # 1.0 == 1 is going to perform correctly all of the time
        # (until the server returns something other than 1 for success, that is)
        first_document[OK] == 1
      end

      # Validate the result by checking for any errors.
      #
      # @note This only checks for errors with writes since authentication is
      #   handled at the connection level and any authentication errors would
      #   be raised there, before a Result is ever created.
      #
      # @example Validate the result.
      #   result.validate!
      #
      # @raise [ Error::OperationFailure ] If an error is in the result.
      #
      # @return [ Result ] The result if verification passed.
      #
      # @since 2.0.0
      # @api private
      def validate!
        !successful? ? raise_operation_failure : self
      end

      # The exception instance (of the Error::OperationFailure class)
      # that would be raised during processing of this result.
      #
      # This method should only be called when result is not successful.
      #
      # @return [ Error::OperationFailure ] The exception.
      #
      # @api private
      def error
        @error ||= Error::OperationFailure.new(
          parser.message,
          self,
          code: parser.code,
          code_name: parser.code_name,
          write_concern_error_document: parser.write_concern_error_document,
          write_concern_error_code: parser.write_concern_error_code,
          write_concern_error_code_name: parser.write_concern_error_code_name,
          write_concern_error_labels: parser.write_concern_error_labels,
          labels: parser.labels,
          wtimeout: parser.wtimeout,
          connection_description: connection_description,
          document: parser.document,
          server_message: parser.server_message,
        )
      end

      # Raises a Mongo::OperationFailure exception corresponding to the
      # error information in this result.
      #
      # @raise Error::OperationFailure
      private def raise_operation_failure
        raise error
      end

      # @return [ TopologyVersion | nil ] The topology version.
      #
      # @api private
      def topology_version
        unless defined?(@topology_version)
          @topology_version = first_document['topologyVersion'] &&
            TopologyVersion.new(first_document['topologyVersion'])
        end
        @topology_version
      end

      # Get the number of documents written by the server.
      #
      # @example Get the number of documents written.
      #   result.written_count
      #
      # @return [ Integer ] The number of documents written.
      #
      # @since 2.0.0
      # @api public
      def written_count
        if acknowledged?
          first_document[N] || 0
        else
          0
        end
      end

      # @api public
      alias :n :written_count

      # Get the operation time reported in the server response.
      #
      # @example Get the operation time.
      #   result.operation_time
      #
      # @return [ Object | nil ] The operation time value.
      #
      # @since 2.5.0
      # @api public
      def operation_time
        first_document && first_document[OPERATION_TIME]
      end

      # Get the cluster time reported in the server response.
      #
      # @example Get the cluster time.
      #   result.cluster_time
      #
      # @return [ ClusterTime | nil ] The cluster time document.
      #
      # Changed in version 2.9.0: This attribute became an instance of
      # ClusterTime, which is a subclass of BSON::Document.
      # Previously it was an instance of BSON::Document.
      #
      # @since 2.5.0
      # @api public
      def cluster_time
        first_document && ClusterTime[first_document['$clusterTime']]
      end

      # Gets the set of error labels associated with the result.
      #
      # @example Get the labels.
      #   result.labels
      #
      # @return [ Array ] labels The set of labels.
      #
      # @since 2.7.0
      # @api private
      def labels
        @labels ||= parser.labels
      end

      # Whether the operation failed with a write concern error.
      #
      # @api private
      def write_concern_error?
        !!(first_document && first_document['writeConcernError'])
      end

      def snapshot_timestamp
        if doc = reply.documents.first
          doc['cursor']&.[]('atClusterTime') || doc['atClusterTime']
        end
      end

      private

      def aggregate_returned_count
        replies.reduce(0) do |n, reply|
          n += reply.number_returned
          n
        end
      end

      def aggregate_written_count
        documents.reduce(0) do |n, document|
          n += (document[N] || 0)
          n
        end
      end

      def parser
        @parser ||= Error::Parser.new(first_document, replies)
      end

      def first_document
        @first_document ||= first || BSON::Document.new
      end

      def query_failure?
        replies.first && (replies.first.query_failure? || replies.first.cursor_not_found?)
      end
    end
  end
end
