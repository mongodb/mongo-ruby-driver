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
  module Response
    module Read

      # Methods for read operations, OP_REPLY messages sent in response to
      # an OP_GET_MORE or an OP_QUERY message.
      #
      # @since 2.0.0
      module Readable

        # Cursor id, to be used for additional get mores.
        #
        # @return [ Integer ] cursor id.
        #
        # @since 2.0.0
        def cursor_id
          0
        end

        # Where in the cursor this reply is starting.
        #
        # @return [ Integer ] the starting point.
        #
        # @since 2.0.0
        def starting_from
          0
        end

        # Get the number of documents in the reply.
        #
        # @return [ Integer ] the number of documents.
        #
        # @since 2.0.0
        def n
          0
        end

        # Get an array of documents returned by this query.
        #
        # @return [ Array ] documents.
        #
        # @since 2.0.0
        def docs
          []
        end

        private

        # No-op, this should be overridden by Readable classes.
        #
        # @return [ Hash ] the message.
        #
        # @since 2.0.0
        def msg
          {}
        end
      end
    end
  end
end
