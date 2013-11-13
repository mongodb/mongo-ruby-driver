# Copyright (C) 2009-2013 MongoDB, Inc.
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

    # MongoDB Wire protocol Delete message.
    #
    # This is a client request message that is sent to the server in order
    # to delete selected documents in the specified namespace.
    #
    # The operation, by default, operates on many documents. Setting
    # the +:single_remove+ flag allows for a single matching document
    # to be removed.
    #
    # @api semipublic
    class Delete < Message

      # Creates a new Delete message
      #
      # @example Remove all users named Tyler.
      #   Query.new('xgen', 'users', {:name => 'Tyler'})
      #
      # @param database [String, Symbol] The database to query.
      # @param collection [String, Symbol] The collection to query.
      # @param selector [Hash] The query selector.
      # @param options [Hash] The additional query options.
      #
      # @option options :skip [Integer] The number of documents to skip.
      # @option options :limit [Integer] The number of documents to return.
      # @option options :fields [Hash] The fields to return.
      # @option options :flags [Array] The flags for the query message.
      #
      #   Supported flags: +:single_remove+
      def initialize(database, collection, selector, options = {})
        @namespace = "#{database}.#{collection}"
        @selector  = selector
        @flags     = options[:flags] || []
      end

      private

      # The operation code required to specify a Delete message.
      # @return [Fixnum] the operation code.
      def op_code
        2006
      end

      # Available flags for a Delete message.
      FLAGS = [:single_remove]

      # Field representing Zero encoded as an Int32.
      field :zero, Zero

      # @!attribute
      # @return [String] The namespace for this Delete message.
      field :namespace, CString

      # @!attribute
      # @return [Array<Symbol>] The flags for this Delete message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [Hash] The selector for this Delete message.
      field :selector, Document
    end
  end
end
