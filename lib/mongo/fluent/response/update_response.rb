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

  # A response object for update operations.  Corresponds to OP_UPDATE.
  #
  # @since 3.0.0
  class UpdateResponse
    include Responsive

    # Parse the 'nModified' field out from a db response to a write command.
    #
    # @return [ Integer ] the number of documents modified by this operation.
    #
    # @since 3.0.0
    def n_modified
      @msg['nModified']
    end
  end
end
