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

    # MongoDB Wire protocol Insert message.
    #
    # This is a client request message that is sent to the server in order
    # to insert documents within a namespace.
    #
    # The operation only has one flag +:continue_on_error+ which the user
    # can use to instruct the database server to continue processing a bulk
    # insertion if one happens to fail (e.g. due to duplicate IDs). This makes
    # builk insert behave similarly to a seires of single inserts, except
    # lastError will be set if any insert fails, not just the last one.
    #
    # If multiple errors occur, only the most recent will be reported by the
    # getLastError mechanism.
    #
    # @api semipublic
    class Insert < Message

      # Creates a new Insert message
      #
      # @example Insert a user document
      #   Insert.new('xgen', 'users', [{:name => 'Tyler'}])
      #
      # @example Insert serveral user documents and continue on errors
      #   Insert.new('xgen', 'users', users, :flags => [:continue_on_error])
      #
      # @param database [String, Symbol]  The database to insert into.
      # @param collection [String, Symbol] The collection to insert into.
      # @param documents [Array<Hash>] The documents to insert.
      # @param options [Hash] Additional options for the insertion.
      #
      # @option options :flags [Array] The flags for the insertion message.
      #
      #   Supported flags: +:continue_on_error+
      def initialize(database, collection, documents, options = {})
        @namespace = "#{database}.#{collection}"
        @documents = documents
        @flags = options[:flags] || []
      end

      # The log message for a insert operation.
      #
      # @example Get the log message.
      #   insert.log_message
      #
      # @return [ String ] The log message
      #
      # @since 2.0.0
      def log_message
        fields = []
        fields << ["%s |", "INSERT"]
        fields << ["namespace=%s", namespace]
        fields << ["documents=%s", documents.inspect]
        fields << ["flags=%s", flags.inspect]
        f, v = fields.transpose
        f.join(" ") % v
      end

      private

      # The operation code required to specify an Insert message.
      # @return [Fixnum] the operation code.
      def op_code
        2002
      end

      # Available flags for an Insert message.
      FLAGS = [:continue_on_error]

      # @!attribute
      # @return [Array<Symbol>] The flags for this Insert message.
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [String] The namespace for this Insert message.
      field :namespace, CString

      # @!attribute
      # @return [Array<Hash>] The documents to insert.
      field :documents, Document, true
    end
  end
end
