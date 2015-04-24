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
  module Protocol

    # MongoDB Wire protocol Query message.
    #
    # This is a client request message that is sent to the server in order
    # to retrieve documents matching provided query.
    #
    # Users may also provide additional options such as a projection, to
    # select a subset of the fields, a number to skip or a limit on the
    # number of returned documents.
    #
    # There are a variety of flags that can be used to adjust cursor
    # parameters or the desired consistancy and integrity the results.
    #
    # @api semipublic
    class Query < Message

      # Constant for the max number of characters to print when inspecting
      # a query field.
      #
      # @since 2.0.3
      LOG_STRING_LIMIT = 250

      # Creates a new Query message
      #
      # @example Find all users named Tyler.
      #   Query.new('xgen', 'users', {:name => 'Tyler'})
      #
      # @example Find all users named Tyler skipping 5 and returning 10.
      #   Query.new('xgen', 'users', {:name => 'Tyler'}, :skip => 5,
      #                                                  :limit => 10)
      #
      # @example Find all users with slave ok bit set
      #   Query.new('xgen', 'users', {:name => 'Tyler'}, :flags => [:slave_ok])
      #
      # @example Find all user ids.
      #   Query.new('xgen', 'users', {}, :fields => {:id => 1})
      #
      # @param database [String, Symbol] The database to query.
      # @param collection [String, Symbol] The collection to query.
      # @param selector [Hash] The query selector.
      # @param options [Hash] The additional query options.
      #
      # @option options :project [Hash] The projection.
      # @option options :skip [Integer] The number of documents to skip.
      # @option options :limit [Integer] The number of documents to return.
      # @option options :flags [Array] The flags for the query message.
      #
      #   Supported flags: +:tailable_cursor+, +:slave_ok+, +:oplog_replay+,
      #   +:no_cursor_timeout+, +:await_data+, +:exhaust+, +:partial+
      def initialize(database, collection, selector, options = {})
        @namespace   = "#{database}.#{collection}"
        @selector    = selector
        @project     = options[:project]
        @skip        = options[:skip]  || 0
        @limit       = options[:limit] || 0
        @flags       = options[:flags] || []
      end

      # The log message for a query operation.
      #
      # @example Get the log message.
      #   query.log_message
      #
      # @return [ String ] The log message
      #
      # @since 2.0.0
      def log_message
        fields = []
        fields << ["%s |", query_type]
        fields << ["namespace=%s", namespace]
        fields << ["selector=%s", formatted_selector]
        fields << ["flags=%s", flags.inspect]
        fields << ["limit=%s", limit.inspect]
        fields << ["skip=%s", skip.inspect]
        fields << ["project=%s", project.inspect]
        f, v = fields.transpose
        f.join(" ") % v
      end

      # Query messages require replies from the database.
      #
      # @example Does the message require a reply?
      #   message.replyable?
      #
      # @return [ true ] Always true for queries.
      #
      # @since 2.0.0
      def replyable?
        true
      end

      private

      # The operation code required to specify a Query message.
      # @return [Fixnum] the operation code.
      def op_code
        2004
      end

      def query_type
        namespace.include?(Database::COMMAND) ? 'COMMAND' : 'QUERY'
      end

      def formatted_selector
        ( (str = selector.inspect).length > LOG_STRING_LIMIT ) ?
          "#{str[0..LOG_STRING_LIMIT]}..." : str
      rescue ArgumentError
        '<Unable to inspect selector>'
      end

      # Available flags for a Query message.
      FLAGS = [
        :reserved,
        :tailable_cursor,
        :slave_ok,
        :oplog_replay,
        :no_cursor_timeout,
        :await_data,
        :exhaust,
        :partial
      ]

      # @!attribute
      # @return [Array<Symbol>] The flags for this query message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [String] The namespace for this query message.
      field :namespace, CString

      # @!attribute
      # @return [Integer] The number of documents to skip.
      field :skip, Int32

      # @!attribute
      # @return [Integer] The number of documents to return.
      field :limit, Int32

      # @!attribute
      # @return [Hash] The query selector.
      field :selector, Document

      # @!attribute
      # @return [Hash] The projection.
      field :project, Document
    end
  end
end
