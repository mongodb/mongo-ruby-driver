# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2017-2020 MongoDB Inc.
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

require 'mongo/collection/view/aggregation/behavior'
require 'mongo/collection/view/change_stream/retryable'

module Mongo
  class Collection
    class View

      # Provides behavior around a `$changeStream` pipeline stage in the
      # aggregation framework. Specifying this stage allows users to request
      # that notifications are sent for all changes to a particular collection
      # or database.
      #
      # @note Only available in server versions 3.6 and higher.
      # @note ChangeStreams do not work properly with JRuby because of the
      #  issue documented here: https://github.com/jruby/jruby/issues/4212.
      #  Namely, JRuby eagerly evaluates #next on an Enumerator in a background
      #  green thread, therefore calling #next on the change stream will cause
      #  getMores to be called in a loop in the background.
      #
      #
      # @since 2.5.0
      class ChangeStream
        include Aggregation::Behavior
        include Retryable

        # @return [ String ] The fullDocument option default value.
        #
        # @since 2.5.0
        FULL_DOCUMENT_DEFAULT = 'default'.freeze

        # @return [ Symbol ] Used to indicate that the change stream should listen for changes on
        #   the entire database rather than just the collection.
        #
        # @since 2.6.0
        DATABASE = :database

        # @return [ Symbol ] Used to indicate that the change stream should listen for changes on
        #   the entire cluster rather than just the collection.
        #
        # @since 2.6.0
        CLUSTER = :cluster

        # @return [ BSON::Document ] The change stream options.
        #
        # @since 2.5.0
        attr_reader :options

        # @return [ Cursor ] the underlying cursor for this operation
        # @api private
        attr_reader :cursor

        # Initialize the change stream for the provided collection view, pipeline
        # and options.
        #
        # @example Create the new change stream view.
        #   ChangeStream.new(view, pipeline, options)
        #
        # @param [ Collection::View ] view The collection view.
        # @param [ Array<Hash> ] pipeline The pipeline of operators to filter the change notifications.
        # @param [ Hash ] options The change stream options.
        #
        # @option options [ String ] :full_document Allowed values: nil, 'default',
        #   'updateLookup', 'whenAvailable', 'required'.
        #
        #   The default is to not send a value (i.e. nil), which is equivalent to
        #   'default'. By default, the change notification for partial updates will
        #   include a delta describing the changes to the document.
        #
        #   When set to 'updateLookup', the change notification for partial updates
        #   will include both a delta describing the changes to the document as well
        #   as a copy of the entire document that was changed from some time after
        #   the change occurred.
        #
        #   When set to 'whenAvailable', configures the change stream to return the
        #   post-image of the modified document for replace and update change events
        #   if the post-image for this event is available.
        #
        #   When set to 'required', the same behavior as 'whenAvailable' except that
        #   an error is raised if the post-image is not available.
        # @option options [ String ] :full_document_before_change Allowed values: nil,
        #   'whenAvailable', 'required', 'off'.
        #
        #   The default is to not send a value (i.e. nil), which is equivalent to 'off'.
        #
        #   When set to 'whenAvailable', configures the change stream to return the
        #   pre-image of the modified document for replace, update, and delete change
        #   events if it is available.
        #
        #   When set to 'required', the same behavior as 'whenAvailable' except that
        #   an error is raised if the pre-image is not available.
        # @option options [ BSON::Document, Hash ] :resume_after Specifies the logical starting point for the
        #   new change stream.
        # @option options [ Integer ] :max_await_time_ms The maximum amount of time for the server to wait
        #   on new documents to satisfy a change stream query.
        # @option options [ Integer ] :batch_size The number of documents to return per batch.
        # @option options [ BSON::Document, Hash ] :collation The collation to use.
        # @option options [ BSON::Timestamp ] :start_at_operation_time Only
        #   return changes that occurred at or after the specified timestamp. Any
        #   command run against the server will return a cluster time that can
        #   be used here. Only recognized by server versions 4.0+.
        # @option options [ Bson::Document, Hash ] :start_after Similar to :resume_after, this
        #   option takes a resume token and starts a new change stream returning the first
        #   notification after the token. This will allow users to watch collections that have been
        #   dropped and recreated or newly renamed collections without missing any notifications.
        # @option options [ Object ] :comment A user-provided
        #   comment to attach to this command.
        # @option options [ Boolean ] :show_expanded_events Enables the server to
        #   send the 'expanded' list of change stream events. The list of additional
        #   events included with this flag set are: createIndexes, dropIndexes,
        #   modify, create, shardCollection, reshardCollection,
        #   refineCollectionShardKey.
        #
        #   The server will report an error if `startAfter` and `resumeAfter` are both specified.
        #
        # @since 2.5.0
        def initialize(view, pipeline, changes_for, options = {})
          # change stream cursors can only be :iterable, so we don't allow
          # timeout_mode to be specified.
          perform_setup(view, options, forbid: %i[ timeout_mode ]) do
            @changes_for = changes_for
            @change_stream_filters = pipeline && pipeline.dup
            @start_after = @options[:start_after]
          end

          # The resume token tracked by the change stream, used only
          # when there is no cursor, or no cursor resume token
          @resume_token = @start_after || @options[:resume_after]

          create_cursor!

          # We send different parameters when we resume a change stream
          # compared to when we send the first query
          @resuming = true
        end

        # Iterate through documents returned by the change stream.
        #
        # This method retries once per error on resumable errors
        # (two consecutive errors result in the second error being raised,
        # an error which is recovered from resets the error count to zero).
        #
        # @example Iterate through the stream of documents.
        #   stream.each do |document|
        #     p document
        #   end
        #
        # @return [ Enumerator ] The enumerator.
        #
        # @since 2.5.0
        #
        # @yieldparam [ BSON::Document ] Each change stream document.
        def each
          raise StopIteration.new if closed?
          loop do
            document = try_next
            yield document if document
          end
        rescue StopIteration
          return self
        end

        # Return one document from the change stream, if one is available.
        #
        # Retries once on a resumable error.
        #
        # Raises StopIteration if the change stream is closed.
        #
        # This method will wait up to max_await_time_ms milliseconds
        # for changes from the server, and if no changes are received
        # it will return nil.
        #
        # @return [ BSON::Document | nil ] A change stream document.
        # @since 2.6.0
        def try_next
          recreate_cursor! if @timed_out

          raise StopIteration.new if closed?

          begin
            doc = @cursor.try_next
          rescue Mongo::Error => e
            # "If a next call fails with a timeout error, drivers MUST NOT
            # invalidate the change stream. The subsequent next call MUST
            # perform a resume attempt to establish a new change stream on the
            # server..."
            #
            # However, SocketTimeoutErrors are TimeoutErrors, but are also
            # change-stream-resumable. To preserve existing (specified) behavior,
            # We only count timeouts when the error is not also
            # change-stream-resumable.
            @timed_out = e.is_a?(Mongo::Error::TimeoutError) && !e.change_stream_resumable?

            raise unless @timed_out || e.change_stream_resumable?

            @resume_token = @cursor.resume_token
            raise e if @timed_out

            recreate_cursor!(@cursor.context)
            retry
          end

          # We need to verify each doc has an _id, so we
          # have a resume token to work with
          if doc && doc['_id'].nil?
            raise Error::MissingResumeToken
          end
          doc
        end

        def to_enum
          enum = super
          enum.send(:instance_variable_set, '@obj', self)
          class << enum
            def try_next
              @obj.try_next
            end
          end
          enum
        end

        # Close the change stream.
        #
        # @example Close the change stream.
        #   stream.close
        #
        # @note This method attempts to close the cursor used by the change
        #   stream, which in turn closes the server-side change stream cursor.
        #   This method ignores any errors that occur when closing the
        #   server-side cursor.
        #
        # @params [ Hash ] opts Options to be passed to the cursor close
        #   command.
        #
        # @return [ nil ] Always nil.
        #
        # @since 2.5.0
        def close(opts = {})
          unless closed?
            begin
              @cursor.close(opts)
            rescue Error::OperationFailure::Family, Error::SocketError, Error::SocketTimeoutError, Error::MissingConnection
              # ignore
            end
            @cursor = nil
          end
        end

        # Is the change stream closed?
        #
        # @example Determine whether the change stream is closed.
        #   stream.closed?
        #
        # @return [ true, false ] If the change stream is closed.
        #
        # @since 2.5.0
        def closed?
          @cursor.nil?
        end

        # Get a formatted string for use in inspection.
        #
        # @example Inspect the change stream object.
        #   stream.inspect
        #
        # @return [ String ] The change stream inspection.
        #
        # @since 2.5.0
        def inspect
          "#<Mongo::Collection::View:ChangeStream:0x#{object_id} filters=#{@change_stream_filters} " +
            "options=#{@options} resume_token=#{resume_token}>"
        end

        # Returns the resume token that the stream will
        # use to automatically resume, if one exists.
        #
        # @example Get the change stream resume token.
        #   stream.resume_token
        #
        # @return [ BSON::Document | nil ] The change stream resume token.
        #
        # @since 2.10.0
        def resume_token
          cursor_resume_token = @cursor.resume_token if @cursor
          cursor_resume_token || @resume_token
        end

        # "change streams are an abstraction around tailable-awaitData cursors..."
        #
        # @return :tailable_await
        def cursor_type
          :tailable_await
        end

        # "change streams...implicitly use ITERATION mode"
        #
        # @return :iteration
        def timeout_mode
          :iteration
        end

        # Returns the value of the max_await_time_ms option that was
        # passed to this change stream.
        #
        # @return [ Integer | nil ] the max_await_time_ms value
        def max_await_time_ms
          options[:max_await_time_ms]
        end

        private

        def for_cluster?
          @changes_for == CLUSTER
        end

        def for_database?
          @changes_for == DATABASE
        end

        def for_collection?
          !for_cluster? && !for_database?
        end

        def create_cursor!(timeout_ms = nil)
          # clear the cache because we may get a newer or an older server
          # (rolling upgrades)
          @start_at_operation_time_supported = nil

          session = client.get_session(@options)
          context = Operation::Context.new(client: client, session: session, view: self, operation_timeouts: timeout_ms ? { operation_timeout_ms: timeout_ms } : operation_timeouts)

          start_at_operation_time = nil
          start_at_operation_time_supported = nil

          @cursor = read_with_retry_cursor(session, server_selector, self, context: context) do |server|
            server.with_connection do |connection|
              start_at_operation_time_supported = connection.description.server_version_gte?('4.0')

              result = send_initial_query(connection, context)

              if doc = result.replies.first && result.replies.first.documents.first
                start_at_operation_time = doc['operationTime']
              else
                # The above may set @start_at_operation_time to nil
                # if it was not in the document for some reason,
                # for consistency set it to nil here as well.
                # NB: since this block may be executed more than once, each
                # execution must write to start_at_operation_time either way.
                start_at_operation_time = nil
              end
              result
            end
          end

          @start_at_operation_time = start_at_operation_time
          @start_at_operation_time_supported = start_at_operation_time_supported
        end

        def pipeline
          [{ '$changeStream' => change_doc }] + @change_stream_filters
        end

        def aggregate_spec(session, read_preference)
          super(session, read_preference).tap do |spec|
            spec[:selector][:aggregate] = 1 unless for_collection?
          end
        end

        def change_doc
          {}.tap do |doc|
            if @options[:full_document]
              doc[:fullDocument] = @options[:full_document]
            end

            if @options[:full_document_before_change]
              doc[:fullDocumentBeforeChange] = @options[:full_document_before_change]
            end

            if @options.key?(:show_expanded_events)
              doc[:showExpandedEvents] = @options[:show_expanded_events]
            end

            if resuming?
              # We have a resume token once we retrieved any documents.
              # However, if the first getMore fails and the user didn't pass
              # a resume token we won't have a resume token to use.
              # Use start_at_operation time in this case
              if resume_token
                # Spec says we need to remove both startAtOperationTime and startAfter if
                # either was passed in by user, thus we won't forward them
                doc[:resumeAfter] = resume_token
              elsif @start_at_operation_time_supported && @start_at_operation_time
                # It is crucial to check @start_at_operation_time_supported
                # here - we may have switched to an older server that
                # does not support operation times and therefore shouldn't
                # try to send one to it!
                #
                # @start_at_operation_time is already a BSON::Timestamp
                doc[:startAtOperationTime] = @start_at_operation_time
              else
                # Can't resume if we don't have either
                raise Mongo::Error::MissingResumeToken
              end
            else
              if @start_after
                doc[:startAfter] = @start_after
              elsif resume_token
                doc[:resumeAfter] = resume_token
              end

              if options[:start_at_operation_time]
                doc[:startAtOperationTime] = time_to_bson_timestamp(
                  options[:start_at_operation_time])
              end
            end

            doc[:allChangesForCluster] = true if for_cluster?
          end
        end

        def send_initial_query(connection, context)
          initial_query_op(context.session, view.read_preference)
            .execute_with_connection(
              connection,
              context: context,
            )
        end

        def time_to_bson_timestamp(time)
          if time.is_a?(Time)
            seconds = time.to_f
            BSON::Timestamp.new(seconds.to_i, ((seconds - seconds.to_i) * 1000000).to_i)
          elsif time.is_a?(BSON::Timestamp)
            time
          else
            raise ArgumentError, 'Time must be a Time or a BSON::Timestamp instance'
          end
        end

        def resuming?
          !!@resuming
        end

        # Recreates the current cursor (typically as a consequence of attempting
        # to resume the change stream)
        def recreate_cursor!(context = nil)
          @timed_out = false

          close
          create_cursor!(context&.remaining_timeout_ms)
        end
      end
    end
  end
end
