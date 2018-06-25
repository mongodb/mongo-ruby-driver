# Copyright (C) 2017 MongoDB, Inc.
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

require 'mongo/collection/view/change_stream/retryable'

module Mongo
  class Collection
    class View

      # Provides behaviour around a `$changeStream` pipeline stage in the
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
      class ChangeStream < Aggregation
        include Retryable

        # @return [ String ] The fullDocument option default value.
        #
        # @since 2.5.0
        FULL_DOCUMENT_DEFAULT = 'default'.freeze

        # @return [ BSON::Document ] The change stream options.
        #
        # @since 2.5.0
        attr_reader :options

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
        # @option options [ String ] :full_document Allowed values: ‘default’, ‘updateLookup’. Defaults to ‘default’.
        #   When set to ‘updateLookup’, the change notification for partial updates will include both a delta
        #   describing the changes to the document, as well as a copy of the entire document that was changed
        #   from some time after the change occurred.
        # @option options [ BSON::Document, Hash ] :resume_after Specifies the logical starting point for the
        #   new change stream.
        # @option options [ Integer ] :max_await_time_ms The maximum amount of time for the server to wait
        #   on new documents to satisfy a change stream query.
        # @option options [ Integer ] :batch_size The number of documents to return per batch.
        # @option options [ BSON::Document, Hash ] :collation The collation to use.
        #
        # @since 2.5.0
        def initialize(view, pipeline, options = {})
          @view = view
          @change_stream_filters = pipeline && pipeline.dup
          @options = options && options.dup.freeze
          @resume_token = @options[:resume_after]
          read_with_one_retry { create_cursor! }
        end

        # Iterate through documents returned by the change stream.
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
          begin
            @cursor.each do |doc|
              cache_resume_token(doc)
              yield doc
            end if block_given?
            @cursor.to_enum
          rescue => e
            close
            if retryable?(e)
              create_cursor!
              retry
            end
            raise
          end
        end

        # Close the change stream.
        #
        # @example Close the change stream.
        #   stream.close
        #
        # @return [ nil ] nil.
        #
        # @since 2.5.0
        def close
          unless closed?
            begin; @cursor.send(:kill_cursors); rescue; end
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
            "options=#{@options} resume_token=#{@resume_token}>"
        end

        private

        def cache_resume_token(doc)
          unless @resume_token = (doc[:_id] && doc[:_id].dup)
            raise Error::MissingResumeToken.new
          end
        end

        def create_cursor!
          session = client.send(:get_session, @options)
          server = server_selector.select_server(cluster)
          result = send_initial_query(server, session)
          @cursor = Cursor.new(view, result, server, disable_retry: true, session: session)
        end

        def pipeline
          change_doc = { fullDocument: ( @options[:full_document] || FULL_DOCUMENT_DEFAULT ) }
          change_doc[:resumeAfter] = @resume_token if @resume_token
          [{ '$changeStream' => change_doc }] + @change_stream_filters
        end

        def send_initial_query(server, session)
          initial_query_op(session).execute(server)
        end
      end
    end
  end
end
