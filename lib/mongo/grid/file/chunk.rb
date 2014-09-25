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

        # @return [ BSON::Document ] document The document stored in the chunks
        #   collection in the database.
        attr_reader :document

        # Create the new chunk.
        #
        # @example Create the chunk.
        #   Chunk.new('testing', file_id, 1)
        #
        # @param [ BSON::Binary ] data The binary chunk data.
        # @param [ BSON::ObjectId ] file_id The id of the file document.
        # @param [ Integer ] sequence The placement of the chunk.
        #
        # @since 2.0.0
        def initialize(data, file_id, sequence)
          @document = BSON::Document.new(
            :_id => BSON::ObjectId.new,
            :files_id => file_id,
            :n => sequence,
            :data => data
          )
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
            chunks, index, sequence = [], 0, 0
            while index < data.length
              chunk = data.slice(index, DEFAULT_SIZE)
              chunks.push(Chunk.new(BSON::Binary.new(chunk), file_id, sequence))
              index += chunk.length
              sequence += 1
            end
            chunks
          end
        end
      end
    end
  end
end
