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
    class File

      # Encapsulates behaviour around GridFS chunks of file data.
      #
      # @since 2.0.0
      class Chunk

        # Name of the chunks collection.
        #
        # @since 2.0.0
        COLLECTION = 'fs_chunks'.freeze

        # Default size for chunks of data.
        #
        # @since 2.0.0
        DEFAULT_SIZE = (255 * 1024).freeze

        # @return [ BSON::Binary ] data The binary chunk data.
        attr_reader :data

        # @return [ BSON::ObjectId ] file_id The file's id.
        attr_reader :file_id

        # @return [ Integer ] position The chunk's position.
        attr_reader :position

        # Get the document for the chunk that would be inserted into the chunks
        # collection.
        #
        # @example Get the chunk document.
        #   chunk.document
        #
        # @return [ BSON::Document ] The chunk as a document.
        #
        # @since 2.0.0
        def document
          @document ||= BSON::Document.new(
            :_id => BSON::ObjectId.new,
            :files_id => file_id,
            :n => position,
            :data => data
          )
        end

        # Create the new chunk.
        #
        # @example Create the chunk.
        #   Chunk.new('testing', file_id, 1)
        #
        # @param [ BSON::Binary ] data The binary chunk data.
        # @param [ BSON::ObjectId ] file_id The id of the file document.
        # @param [ Integer ] position The placement of the chunk.
        #
        # @since 2.0.0
        def initialize(data, file_id, position)
          @data = data
          @file_id = file_id
          @position = position
        end

        # Conver the chunk to BSON for storage.
        #
        # @example Convert the chunk to BSON.
        #   chunk.to_bson
        #
        # @param [ String ] encoded The encoded data to append to.
        #
        # @return [ String ] The raw BSON data.
        #
        # @since 2.0.0
        def to_bson(encoded = ''.force_encoding(BSON::BINARY))
          document.to_bson(encoded)
        end

        class << self

          def assemble(chunks)

          end

          # Split the provided data into multiple chunks.
          #
          # @example Split the data into chunks.
          #   Chunks.split(data)
          #
          # @param [ String ] data The raw bytes.
          # @param [ BSON::ObjectId ] file_id The file id.
          #
          # @return [ Array<Chunk> ] The chunks of the data.
          #
          # @since 2.0.0
          def split(data, file_id)
            chunks, index, position = [], 0, 0
            while index < data.length
              chunk = data.slice(index, DEFAULT_SIZE)
              chunks.push(Chunk.new(BSON::Binary.new(chunk), file_id, position))
              index += chunk.length
              position += 1
            end
            chunks
          end
        end
      end
    end
  end
end
