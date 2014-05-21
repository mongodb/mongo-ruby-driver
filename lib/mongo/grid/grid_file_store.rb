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
  module Grid

    # Implementation of basic MongoDB GridFS.
    #
    # @since 3.0.0
    class GridFileStore
      include Storable

      # Create a new Grid object
      #
      # @param [ Mongo::Collection ] files A collection for metadata.
      # @param [ Mongo::Collection ] chunks A collection for the files themselves.
      #
      # @since 3.0.0
      def initialize(files, chunks)
        @files = files
        @chunks = chunks
        # @todo db - create indexes on these collections.
      end
    end
  end
end
