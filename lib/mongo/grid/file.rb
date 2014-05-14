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

    # Grid::File supports operations on files stored within a Grid::FS.
    #
    # @note Users should not need to instantiate this class directly.
    #
    # @since 3.0.0
    class File

      # @return [ BSON::ObjectId ] Unique identifier for this file.
      attr_reader :files_id
      # @return [ String ] The mode of this file.
      attr_reader :mode

      # Opens the file identified by id according to the given mode.
      #
      # @param [ String, BSON::ObjectId ] id
      # @param [ Mongo::Collection ] files A collection for metadata.
      # @param [ Mongo::Collection ] chunks A collection for file data.
      # @param [ Hash ] opts Options for this file.
      #
      # @options opts [ Integer ] (261120) :chunk_size Custom chunk size, in bytes.
      # @options opts [ Array ] :aliases Array of alias strings for this filename.
      # @options opts [ String ] :content_type A valid MIME type for this document.
      # @options opts [ BSON::ObjectId ] :_id A custom files_id for this file.
      # @options opts [ Hash ] :metadata Any additional metadata for this file.
      #
      # @since 3.0.0
      def initialize(id, mode, files, chunks, opts={})
        @files         = files
        @chunks        = chunks
        @mode          = mode
        @file_position = 0

        if mode == 'r'
          init_read(id)
        elsif mode == 'w'
          init_write(id, opts)
        else
          raise "invalid mode #{mode}"
        end
        @current_chunk = get_chunk(0)
        @local_md5     = files_doc[:md5]
      end
      alias :open :initialize

      # Return the length of this file, in characters.
      #
      # @return [ Integer ] length of the file.
      #
      # @since 3.0.0
      def size
        files_doc[:length]
      end

      # Read from the file. If length is specified, read length characters starting
      # from the current file position. Otherwise, read from the current file position
      # until eof.
      #
      # @param [ Integer ] length Number of characters to read.
      #
      # @return [ String ] file data.
      #
      # @since 3.0.0
      def read(length=nil)
        # @todo add a seek option
        ensure_mode('r') do
          length ||= files_doc[:length] - @file_position
          return read_string(length)
        end
      end
      alias :data :read

      # Write data to the file, beginning at the current eof.
      #
      # @param [ String, BSON::ObjectId ] io Data to write.
      #
      # @return [ Integer ] The number of bytes written.
      #
      # @since 3.0.0
      def write(io)
        bytes_written = 0
        ensure_mode('w') do
         if io.is_a?(String)
            bytes_written = write_string(io)
          else
            while msg = io.read(files_doc[:chunkSize])
              bytes_written += write_string(msg)
            end
          end
        end
        bytes_written
      end

      # Check equality of two Grid::File objects.
      #
      # @param [ Grid::File ] other The other Grid::File object.
      #
      # @return [ true, false ] Are the objects equal?
      #
      # @since 3.0.0
      def ==(other)
        return false unless other.is_a?(Grid::File)
        other.files_id == @files_id && other.mode == @mode
      end

      private

      # Initialize a file opened in 'r' mode.
      #
      # @param [ BSON::ObjectId, String ] id An identifier for this file.
      #
      # @since 3.0.0
      def init_read(id)
        metadata = files_doc(id)
        raise GridError, "File #{id} not found, could not open" unless metadata
        @files_id = metadata[:_id]
      end

      # Initialize a file opened in 'w' mode.
      #
      # @param [ BSON::ObjectId, String ] id An identifier for this file.
      #
      # @since 3.0.0
      def init_write(id, opts={})
        metadata = files_doc(id)
        if !metadata
          if id.is_a?(BSON::ObjectId)
            raise GridError, "File #{id} not found, could not open"
          else
            init_new_file(id, opts)
          end
        else
          @files_id = metadata[:_id]
          truncate(@files_id)
        end
      end

      # Create and save a files collection entry for a new file.
      #
      # @param [ String ] filename The name of the file.
      #
      # @since 3.0.0
      def init_new_file(filename, opts={})
        # @todo options for chunkSize, alias, contentType, metadata
        @files_id = opts[:_id] || BSON::ObjectId.new
        @files.save({ :_id         => @files_id,
                      :chunkSize   => opts[:chunk_size] || DEFAULT_CHUNK_SIZE,
                      :filename    => filename,
                      :md5         => Digest::MD5.new,
                      :length      => 0,
                      :uploadDate  => Time.now.utc,
                      :contentType => opts[:content_type] || DEFAULT_CONTENT_TYPE,
                      :aliases     => opts[:aliases]      || [],
                      :metadata    => opts[:metadata]     || {} })
      end

      # Read a string of data from the file's chunks
      #
      # @param [ Integer ] length Number of characters to read.
      #
      # @return [ String ] file data.
      #
      # @since 3.0.0
      def read_string(length)
        metadata   = files_doc
        remaining  = metadata[:length] - @file_position
        length     = remaining if length > remaining
        bytes_read = 0
        buf        = ''

        while bytes_read < length
          break unless chunk = next_chunk

          chunk_offset = @file_position % metadata[:chunkSize]
          to_read = metadata[:chunkSize] - chunk_offset
          to_read = length - bytes_read if to_read > length - bytes_read
          buf << chunk[:data][chunk_offset, to_read]
          bytes_read += to_read
          @file_position += to_read
        end
        buf.empty? ? nil : buf
      end

      # Write 'data' to the file.
      #
      # @param [ String ] msg Data to write.
      #
      # @return [ Integer ] number of characters written.
      #
      # @since 3.0.0
      def write_string(data)
        bytes_written = 0
        metadata = files_doc
        while bytes_written < data.length
          chunk = next_chunk
          free_space = metadata[:chunkSize]
          bytes_left = data.length - bytes_written
          to_write = bytes_left <= free_space ? bytes_left : free_space

          chunk[:data] << data[bytes_written, to_write]
          bytes_written += to_write
          @file_position += to_write
          save_chunk(chunk)
        end
        @local_md5.update(data)
        bytes_written
      end

      # Return the nth chunk of this file.
      #
      # @param [ Integer ] n The nth chunk.
      #
      # @return [ Hash ] the nth chunk.
      def get_chunk(n)
        chunk = @chunks.find_one({ :files_id => @files_id, :n => n })
        if mode == 'w'
          return chunk || new_chunk(n)
        else
          return chunk
        end
      end

      # Create a new, empty chunk at index 'n', save to db.
      #
      # @param [ Integer ] n Index of this chunk.
      #
      # @return [ Hash ] the nth chunk.
      def new_chunk(n)
        chunk = { :_id      => BSON::ObjectId.new,
                  :files_id => @files_id,
                  :n        => n,
                  :data     => '' }
        @chunks.save(chunk)
        chunk
      end

      # Return the next chunk from the database, or from our cache.
      #
      # @return [ Hash ] the next chunk.
      def next_chunk
        n = (@file_position / files_doc[:chunkSize]).floor
        if @current_chunk && @current_chunk[:n] == n
          @current_chunk
        else
          @current_chunk = get_chunk(n)
        end
      end

      # Save this chunk to the database.
      #
      # @param [ Hash ] chunk
      #
      # @since 3.0.0
      def save_chunk(chunk)
        @chunks.save(chunk)
      end

      # Update this file's metadata.
      # @note this can only be used while in 'w' mode
      #
      # @since 3.0.0
      def update_metadata
        # @todo db - refactor to use an update
        metadata = files_doc
        metadata[:length] = @file_position
        metadata[:md5] = @local_md5
        @files.save(metadata)
      end

      # Raise an error if this file is not in the correct mode.
      #
      # @since 3.0.0
      def ensure_mode(mode)
        raise GridError, "Mode must be #{mode}" unless @mode == mode
        yield
        update_metadata if mode == 'w'
        # @todo validate_write if @write_concern
      end

      # Truncate existing file.
      #
      # @param [ BSON::ObjectId ] files_id
      #
      # @since 3.0.0
      def truncate(id)
        # @todo db - refactor to use an update
        metadata = files_doc
        metadata[:length] = 0
        @files.save(metadata)
        @chunks.remove({ :files_id => id })
      end

      # Given an identifier for this file, return its metadata document.
      #
      # @param [ BSON::ObjectId, String ] id An Identifier for this file.
      #
      # @return [ Hash ] metadata document.
      #
      # @since 3.0.0
      def files_doc(id=@files_id)
        if id.is_a?(BSON::ObjectId)
          @files.find_one({ :_id => id })
        else
          @files.find_one({ :filename => id })
        end
      end
    end
  end
end
