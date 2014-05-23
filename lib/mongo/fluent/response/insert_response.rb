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

    # A response object for insert operations, OP_INSERT.
    #
    # @since 3.0.0
    class InsertResponse
      include Responsive

      # Parse out the 'nInserted' field from a db response to a write command.
      #
      # @return [ Integer ] number of documents inserted.
      #
      # @since 3.0.0
      def n_inserted
        @msg['nInserted']
      end
    end
  end
end
