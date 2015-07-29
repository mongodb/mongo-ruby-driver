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
          # @since 2.1.0
          def initialize(fs, options)
            @fs = fs
            @options = options
            @file_id = options[:file_id]
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
          # @raise [ Error::UnexpectedChunkN ] If a chunk is found out of sequence.
          #
          # @yieldparam [ Hash ] Each chunk data.
          #
          # @since 2.1.0
          def each
            ensure_readable!
            view.each_with_index do |doc, index|
              chunk = Grid::File::Chunk.new(doc)
              validate_n!(index, chunk)
              data = Grid::File::Chunk.assemble([ chunk ])
              yield data
            end if block_given?
            view.to_enum
          end

          # Read all file data.
          #
          # @example Read the file data.
          #   stream.read
          #
          # @return [ String ] The file data.
          #
          # @raise [ Error::UnexpectedChunkN ] If a chunk is found out of sequence.
          #
          # @since 2.1.0
          def read
            to_a.join
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
            @open = false
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
          # @return [ Mongo::ServerSelector] The read preference.
          #
          # @since 2.1.0
          def read_preference
            @read_preference ||= @options[:read] ?
                ServerSelector.get((@options[:read] || {}).merge(fs.options)) :
                fs.read_preference
          end

          # Get the files collection file information document for the file being read.
          #
          # @example Get the file info document.
          #   stream.file_info
          #
          # @return [ Hash ] The file info document.
          #
          # @since 2.1.0
          def file_info
            @file_info ||= fs.files_collection.find(_id: file_id).first
          end

          private

          def ensure_open!
            raise Error::ClosedStream.new if closed?
          end

          def ensure_file_info!
            raise Error::NoFileInfo.new unless file_info
          end

          def ensure_readable!
            ensure_open!
            ensure_file_info!
          end

          def view
            @view ||= fs.chunks_collection.find({ :files_id => file_id }, options).read(read_preference).sort(:n => 1)
          end

          def validate_n!(index, chunk)
            unless index == chunk.n
              close
              raise Error::UnexpectedChunkN.new(index, chunk)
            end
          end
        end
      end
    end
  end
end
