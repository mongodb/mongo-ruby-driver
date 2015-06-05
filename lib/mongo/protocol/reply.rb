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

    # The MongoDB wire protocol message representing a reply
    #
    # @example
    #   socket = TCPSocket.new('localhost', 27017)
    #   query = Protocol::Query.new('xgen', 'users', {:name => 'Tyler'})
    #   socket.write(query)
    #   reply = Protocol::Reply::deserialize(socket)
    #
    # @api semipublic
    class Reply < Message

      # Determine if the reply had a query failure flag.
      #
      # @example Did the reply have a query failure.
      #   reply.query_failure?
      #
      # @return [ true, false ] If the query failed.
      #
      # @since 2.0.5
      def query_failure?
        flags.include?(:query_failure)
      end

      private

      # The operation code required to specify a Reply message.
      # @return [Fixnum] the operation code.
      def op_code
        1
      end

      # Available flags for a Reply message.
      FLAGS = [
        :cursor_not_found,
        :query_failure,
        :shard_config_stale,
        :await_capable
      ]

      public

      # @!attribute
      # @return [Array<Symbol>] The flags for this reply.
      #
      #   Supported flags: +:cursor_not_found+, +:query_failure+,
      #   +:shard_config_stale+, +:await_capable+
      field :flags, BitVector.new(FLAGS)

      # @!attribute
      # @return [Fixnum] The cursor id for this response. Will be zero
      #   if there are no additional results.
      field :cursor_id, Int64

      # @!attribute
      # @return [Fixnum] The starting position of the cursor for this Reply.
      field :starting_from, Int32

      # @!attribute
      # @return [Fixnum] Number of documents in this Reply.
      field :number_returned, Int32

      # @!attribute
      # @return [Array<Hash>] The documents in this Reply.
      field :documents, Document, :@number_returned
    end
  end
end
