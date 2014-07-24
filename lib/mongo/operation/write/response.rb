# Copyright (C) 2009-2014 MongoDB, Inc.
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
    module Write

      # Wraps responses from the database to provide a common interface between
      # legacy and current MongoDB installations.
      #
      # @since 2.0.0
      class Response

        # The number of documents updated in the write.
        #
        # @since 2.0.0
        N = 'n'.freeze

        # The ok status field in the response.
        #
        # @since 2.0.0
        OK = 'ok'.freeze

        # The standard ok document returned from the database.
        #
        # @since 2.0.0
        DOCUMENT = { OK => 1, N => 1 }.freeze

        # Get the documents from the reply.
        #
        # @example Get the documents.
        #   response.documents
        #
        # @return [ Array<Hash> ] The documents in the response.
        #
        # @since 2.0.0
        def documents
          reply ? reply.documents : []
        end

        # Initialize the response from the wire protocol reply. This could be nil
        # in the case of writes on legacy MongoDB installations (before 2.6) if
        # the GLE is not being used.
        #
        # @example Initialize the response.
        #   Response.new(reply)
        #
        # @note Once support for MongoDB < 2.6 is removed we can refactor this
        #   to only take a reply and no nils or counts.
        #
        # @param [ Protocol::Reply, nil, Integer ] reply_or_count The reply from
        #   the database or count of successfule documents written.
        #
        # @since 2.0.0
        def initialize(reply_or_count)
          if reply_or_count.is_a?(Integer)
            @n = reply_or_count
          else
            @reply = reply_or_count
            verify!
          end
        end

        # Get the pretty formatted inspection of the response.
        #
        # @example Inspect the response.
        #   response.inspect
        #
        # @return [ String ] The inspection.
        #
        # @since 2.0.0
        def inspect
          "#<Mongo::Operation::Write::Response:#{object_id} written=#{n} documents=#{documents}>"
        end

        # Gets the number of documents that were written in this operation.
        #
        # @example Get the written count.
        #   response.n
        #
        # @return [ Integer ] The number of documents written to.
        #
        # @since 2.0.0
        def n
          @n || (reply ? first[N] : nil)
        end

        private

        attr_reader :reply

        def first
          @first ||= reply.documents[0]
        end

        def command_failure?
          first[OK] != 1 || write_errors?
        end

        def verify!
          raise Failure.new(first) if reply && command_failure?
        end

        def write_concern_errors
          first[CONCERN_ERROR] || []
        end

        def write_concern_errors?
          !write_concern_errors.empty?
        end

        def write_errors
          first[ERRORS] || []
        end

        def write_errors?
          !write_errors.empty?
        end
      end
    end
  end
end
