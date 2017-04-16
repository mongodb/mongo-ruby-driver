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
    module Write

      # Methods for parsing out fields for a response to a write command.
      #
      # @since 2.0.0
      module Writable

        # Parse out the 'nInserted' field from a db response to a write command.
        #
        # @return [ Integer ] number of documents inserted.
        #
        # @since 2.0.0
        def n_inserted
          0
        end

        # Parse out the 'nRemoved' field from a db response to a write command.
        #
        # @return [ Integer ] number of documents removed.
        #
        # @since 2.0.0
        def n_removed
          0
        end

        # Parse out the 'nMatched' field from a db response to a write command.
        #
        # @return [ Integer ] number of documents matching the query selector.
        #
        # @since 2.0.0
        def n_matched
          0
        end

        # Parse out the 'nModified' field from a db response to a write command.
        #
        # @return [ Integer ] number of documents modified.
        #
        # @since 2.0.0
        def n_modified
          0
        end

        # Parse out the 'nUpserted' field from a db response to a write command.
        #
        # @return [ Integer ] number of documents upserted.
        #
        # @since 2.0.0
        def n_upserted
          0
        end

        # Did this operation encounter a write concern error?
        #
        # @return [ true, false ] success of this operation.
        #
        # @since 2.0.0
        def write_concern_error?
          false
        end

        # Return the write concern error for this operation, if there was one.
        #
        # @return [ Mongo::Response::WriteConcernError ]
        #
        # @since 2.0.0
        def write_concern_error
          nil
        end

        # Did this operation encounter a write error?
        #
        # @return [ true, false ] success of the operation.
        #
        # @since 2.0.0
        def write_error?
          false
        end

        # Return the write error for this operation, if there was one.
        #
        # @return [ Mongo::Response::WriteError ]
        #
        # @since 2.0.0
        def write_error
          nil
        end
      end
    end
  end
end
