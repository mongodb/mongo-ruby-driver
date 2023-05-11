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

module Mongo
  module Operation
    class Insert

      # Defines custom behavior of results for an insert when sent as part of a bulk write.
      #
      # @since 2.0.0
      # @api semiprivate
      class BulkResult < Operation::Result
        include Aggregatable

        # Get the ids of the inserted documents.
        #
        # @since 2.0.0
        # @api public
        attr_reader :inserted_ids

        # Initialize a new result.
        #
        # @example Instantiate the result.
        #   Result.new(replies, inserted_ids)
        #
        # @param [ Array<Protocol::Message> | nil ] replies The wire protocol replies, if any.
        # @param [ Server::Description ] connection_description
        #   Server description of the server that performed the operation that
        #   this result is for.
        # @param [ Integer ] connection_global_id
        #   Global id of the connection on which the operation that
        #   this result is for was performed.
        # @param [ Array<Object> ] ids The ids of the inserted documents.
        #
        # @since 2.0.0
        # @api private
        def initialize(replies, connection_description, connection_global_id, ids)
          @replies = [*replies] if replies
          @connection_description = connection_description
          @connection_global_id = connection_global_id
          if replies && replies.first && (doc = replies.first.documents.first)
            if errors = doc['writeErrors']
              # some documents were potentially inserted
              bad_indices = {}
              errors.map do |error|
                bad_indices[error['index']] = true
              end
              @inserted_ids = []
              ids.each_with_index do |id, index|
                if bad_indices[index].nil?
                  @inserted_ids << id
                end
              end
            # I don't know if acknowledged? check here is necessary,
            # as best as I can tell it doesn't hurt
            elsif acknowledged? && successful?
              # We have a reply and the reply is successful and the
              # reply has no writeErrors - everything got inserted
              @inserted_ids = ids
            else
              # We have a reply and the reply is not successful and
              # it has no writeErrors - nothing got inserted.
              # If something got inserted the reply will be not successful
              # but will have writeErrors
              @inserted_ids = []
            end
          else
            # I don't think we should ever get here but who knows,
            # make this behave as old drivers did
            @inserted_ids = ids
          end
        end

        # Gets the number of documents inserted.
        #
        # @example Get the number of documents inserted.
        #   result.n_inserted
        #
        # @return [ Integer ] The number of documents inserted.
        #
        # @since 2.0.0
        # @api public
        def n_inserted
          written_count
        end

        # Gets the id of the document inserted.
        #
        # @example Get id of the document inserted.
        #   result.inserted_id
        #
        # @return [ Object ] The id of the document inserted.
        #
        # @since 2.0.0
        # @api public
        def inserted_id
          inserted_ids.first
        end
      end
    end
  end
end
