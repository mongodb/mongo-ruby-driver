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

require 'mongo/grid/file/chunk'
require 'mongo/grid/file/metadata'

module Mongo
  module Grid

    # A representation of a file in the database.
    #
    # @since 2.0.0
    class File

      # @return [ Array<Chunk> ] chunks The file chunks.
      attr_reader :chunks

      # @return [ Metadata ] metadata The file metadata.
      attr_reader :metadata

      # Initialize the file.
      #
      # @example Create the file.
      #   Grid::File.new(data, :filename => 'test.txt')
      #
      # @param [ IO, Array<Chunk> ] data The file or IO object.
      # @param [ BSON::Document ] The metadata document.
      #
      # @since 2.0.0
      def initialize(data, document)
        @metadata = Metadata.new({ :length => data.length }.merge(document))
        initialize_chunks(data)
      end

      private

      def initialize_chunks(data)
        if data.is_a?(Array)
          @chunks = data
          @data = Chunk.assemble(data)
        else
          @chunks = Chunk.split(data, metadata.id)
          @data = data
        end
      end
    end
  end
end
