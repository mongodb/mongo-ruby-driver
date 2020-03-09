# Copyright (C) 2014-2020 MongoDB Inc.
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

          # @return [ Hash ] options The stream options.
          #
          # @since 2.1.0
          attr_reader :options

          # @return [ BSON::ObjectId, Object ] file_id The id of the file being read.
          #
          # @since 2.1.0
          attr_reader :file_id

          # Create a stream for reading files from the FSBucket.
          #
          # @example Create the stream.
          #   Stream::Read.new(fs, options)
          #
          # @param [ FSBucket ] fs The GridFS bucket object.
          # @param [ Hash ] options The read stream options.
          #
          # @option options [ BSON::Document ] :file_info_doc For internal
          #   driver use only. A BSON document to use as file information.
          #
          # @since 2.1.0
          def initialize(fs, options)
            @fs = fs
            @options = options.dup
            @file_id = @options.delete(:file_id)
            @options.freeze
            @open = true
          end

          # Iterate through chunk data streamed from the FSBucket.
          #
          # @example Iterate through the chunk data.
          #   stream.each do |data|
          #     buffer << data
          #   end
          #
          # @return [ Enumerator ] The enumerator.
          #
          # @raise [ Error::MissingFileChunk ] If a chunk is found out of sequence.
          #
          # @yieldparam [ Hash ] Each chunk of file data.
          #
          # @since 2.1.0
          def each
            ensure_readable!
            info = file_info
            num_chunks = (info.length + info.chunk_size - 1) / info.chunk_size
            if block_given?
              view.each_with_index.reduce(0) do |length_read, (doc, index)|
                chunk = Grid::File::Chunk.new(doc)
                validate!(index, num_chunks, chunk, length_read)
                data = chunk.data.data
                yield data
                length_read += data.size
              end
            else
              view.to_enum
            end
          end

          # Read all file data.
          #
          # @example Read the file data.
          #   stream.read
          #
          # @return [ String ] The file data.
          #
          # @raise [ Error::MissingFileChunk ] If a chunk is found out of sequence.
          #
          # @since 2.1.0
          def read
            to_a.join
          end

          # Close the read stream.
          #
          # If the stream is already closed, this method does nothing.
          #
          # @example Close the stream.
          #   stream.close
          #
          # @return [ BSON::ObjectId, Object ] The file id.
          #
          # @since 2.1.0
          def close
            if @open
              view.close_query
              @open = false
            end
            file_id
          end

          # Is the stream closed.
          #
          # @example Is the stream closd.
          #   stream.closed?
          #
          # @return [ true, false ] Whether the stream is closed.
          #
          # @since 2.1.0
          def closed?
            !@open
          end

          # Get the read preference used when streaming.
          #
          # @example Get the read preference.
          #   stream.read_preference
          #
          # @return [ Mongo::ServerSelector ] The read preference.
          #
          # @since 2.1.0
          def read_preference
            @read_preference ||= options[:read] || fs.read_preference
          end

          # Get the files collection file information document for the file
          # being read.
          #
          # @note The file information is cached in the stream. Subsequent
          #   calls to file_info will return the same information that the
          #   first call returned, and will not query the database again.
          #
          # @return [ File::Info ] The file information object.
          #
          # @since 2.1.0
          def file_info
            @file_info ||= begin
              doc = options[:file_info_doc] || fs.files_collection.find(_id: file_id).first
              if doc
                File::Info.new(Options::Mapper.transform(doc, File::Info::MAPPINGS.invert))
              else
                nil
              end
            end
          end

          private

          def ensure_open!
            raise Error::ClosedStream.new if closed?
          end

          def ensure_file_info!
            raise Error::FileNotFound.new(file_id, :id) unless file_info
          end

          def ensure_readable!
            ensure_open!
            ensure_file_info!
          end

          def view
            @view ||= begin
              opts = if read_preference
                options.merge(read: read_preference)
              else
                options
              end

              fs.chunks_collection.find({ :files_id => file_id }, opts).sort(:n => 1)
            end
          end

          def validate!(index, num_chunks, chunk, length_read)
            validate_n!(index, chunk)
            validate_length!(index, num_chunks, chunk, length_read)
          end

          def raise_unexpected_chunk_length!(chunk)
            close
            raise Error::UnexpectedChunkLength.new(file_info.chunk_size, chunk)
          end

          def validate_length!(index, num_chunks, chunk, length_read)
            if num_chunks > 0 && chunk.data.data.size > 0
              raise Error::ExtraFileChunk.new unless index < num_chunks
              if index == num_chunks - 1
                unless chunk.data.data.size + length_read == file_info.length
                  raise_unexpected_chunk_length!(chunk)
                end
              elsif chunk.data.data.size != file_info.chunk_size
                raise_unexpected_chunk_length!(chunk)
              end
            end
          end

          def validate_n!(index, chunk)
            unless index == chunk.n
              close
              raise Error::MissingFileChunk.new(index, chunk)
            end
          end
        end
      end
    end
  end
end
