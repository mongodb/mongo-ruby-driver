# Copyright (C) 2014-2015 MongoDB, Inc.
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

require 'stringio'

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
        COLLECTION = 'chunks'.freeze

        # Default size for chunks of data.
        #
        # @since 2.0.0
        DEFAULT_SIZE = (255 * 1024).freeze

        # @return [ BSON::Document ] document The document to store for the
        #   chunk.
        attr_reader :document

        # Check chunk equality.
        #
        # @example Check chunk equality.
        #   chunk == other
        #
        # @param [ Object ] other The object ot compare to.
        #
        # @return [ true, false ] If the objects are equal.
        #
        # @since 2.0.0
        def ==(other)
          return false unless other.is_a?(Chunk)
          document == other.document
        end

        # Get the BSON type for a chunk document.
        #
        # @example Get the BSON type.
        #   chunk.bson_type
        #
        # @return [ Integer ] The BSON type.
        #
        # @since 2.0.0
        def bson_type
          BSON::Hash::BSON_TYPE
        end

        # Get the chunk data.
        #
        # @example Get the chunk data.
        #   chunk.data
        #
        # @return [ BSON::Binary ] The chunk data.
        #
        # @since 2.0.0
        def data
          document[:data]
        end

        # Get the chunk id.
        #
        # @example Get the chunk id.
        #   chunk.id
        #
        # @return [ BSON::ObjectId ] The chunk id.
        #
        # @since 2.0.0
        def id
          document[:_id]
        end

        # Get the files id.
        #
        # @example Get the files id.
        #   chunk.files_id
        #
        # @return [ BSON::ObjectId ] The files id.
        #
        # @since 2.0.0
        def files_id
          document[:files_id]
        end

        # Get the chunk position.
        #
        # @example Get the chunk position.
        #   chunk.n
        #
        # @return [ Integer ] The chunk position.
        #
        # @since 2.0.0
        def n
          document[:n]
        end

        # Create the new chunk.
        #
        # @example Create the chunk.
        #   Chunk.new(document)
        #
        # @param [ BSON::Document ] document The document to create the chunk
        #   from.
        #
        # @since 2.0.0
        def initialize(document)
          @document = BSON::Document.new(:_id => BSON::ObjectId.new).merge(document)
        end

        # Conver the chunk to BSON for storage.
        #
        # @example Convert the chunk to BSON.
        #   chunk.to_bson
        #
        # @param [ String ] buffer The encoded data buffer to append to.
        #
        # @return [ String ] The raw BSON data.
        #
        # @since 2.0.0
        def to_bson(buffer = BSON::ByteBuffer.new)
          document.to_bson(buffer)
        end

        class << self

          # Takes an array of chunks and assembles them back into the full
          # piece of raw data.
          #
          # @example Assemble the chunks.
          #   Chunk.assemble(chunks)
          #
          # @param [ Array<Chunk> ] chunks The chunks.
          #
          # @return [ String ] The assembled data.
          #
          # @since 2.0.0
          def assemble(chunks)
            chunks.reduce(''){ |data, chunk| data << chunk.data.data }
          end

          # Split the provided data into multiple chunks.
          #
          # @example Split the data into chunks.
          #   Chunks.split(data)
          #
          # @param [ String, IO ] data The raw bytes.
          # @param [ File::Info ] file_info The files collection file doc.
          #
          # @return [ Array<Chunk> ] The chunks of the data.
          #
          # @since 2.0.0
          def split(io, file_info, offset = 0)
            io = StringIO.new(io) if io.is_a?(String)
            parts = Enumerator.new { |y| y << io.read(file_info.chunk_size) until io.eof? }
            parts.map.with_index do |bytes, n|
              file_info.md5.update(bytes)
              Chunk.new(
                data: BSON::Binary.new(bytes),
                files_id: file_info.id,
                n: n + offset,
              )
            end
          end
        end
      end
    end
  end
end
