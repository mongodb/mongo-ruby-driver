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

module Mongo
  module Grid
    class FSBucket

      module Stream
        # A stream that reads files from the FSBucket.
        #
        # @since 2.1.0
        class Read
          include Enumerable

          # @return [ FSBucket ] fs The fs bucket from which this stream reads.
          #
          # @since 2.1.0
          attr_reader :fs

          # @return [ BSON::ObjectId, Object ] The file id.
          #
          # @since 2.1.0
          attr_reader :id

          # @return [ Hash ] The read stream options.
          #
          # @since 2.1.0
          attr_reader :options

          # Create a stream for reading files from the FSBucket.
          #
          # @example Create the stream.
          #   Stream::Read.new(fs, options)
          #
          # @param [ FSBucket ] fs The GridFS bucket object.
          # @param [ Hash ] options The read stream options.
          #
          # @since 2.1.0
          def initialize(fs, options)
            @fs = fs
            @options = options
            @closed = false
          end

          # Iterate through chunk data streamed from the FSBucket.
          #
          # @example Iterate through the chunk data.
          #   read.each do |data|
          #     buffer << data
          #   end
          #
          # @return [ Enumerator ] The enumerator.
          #
          # @raise [ Error::UnexpectedChunkN ] If a chunk is found out of sequence.
          #
          # @since 2.1.0
          #
          # @yieldparam [ Hash ] Each matching document.
          def each
            ensure_open!
            view.each_with_index do |doc, index|
              chunk = Grid::File::Chunk.new(doc)
              validate_n!(chunk, index)
              data = Grid::File::Chunk.assemble([ chunk ])
              yield data
            end if block_given?
            view.to_enum
          end

          # Close the read stream.
          #
          # @example Close the stream.
          #   stream.close
          #
          # @return [ true ] true.
          #
          # @raise [ Error::ClosedStream ] If the stream is already closed.
          #
          # @since 2.1.0
          def close
            ensure_open!
            view.close_query
            @closed = true
          end

          private

          def view
            @view ||= fs.chunks_collection.find({ :files_id => id }, options).sort(:n => 1)
          end

          def ensure_open!
            raise Error::ClosedStream.new if @closed
          end

          def id
            @id ||= options[:id]
          end

          def validate_n!(chunk, index)
            raise UnexpectedChunkN.new(index, chunk) unless chunk.n == index
          end
        end
      end
    end
  end
end
