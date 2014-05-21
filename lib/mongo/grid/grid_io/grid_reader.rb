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
    module GridIO

      # A file-like object with methods to support the 'r' mode of Storable
      # classes.
      #
      # @since 3.0.0
      class GridReader

        # @return [ BSON::ObjectId ] Unique identifier for this file.
        attr_reader :files_id
        # @return [ Integer ] Current file position.
        attr_reader :file_position
        # @return [ Integer ] Current length of this file.
        attr_reader :file_length

        # Create a new GridReader object.
        #
        # @note Users should not need to instantiate this class directly.
        #
        # @param [ Mongo::Collection ] files A collection for storing file metadata.
        # @param [ Mongo::Collection ] chunks A collection for storing chunks of files.
        # @param [ BSON::ObjectId, String ] key Either the filename or the unique
        #   files_id for this file. If a String is used, returns the first file found
        #   with that name.
        # @param [ Hash ] opts Additional options for this GridReader instance.
        #
        # @options opts [ String ] :fs_name Custom file system prefix
        # @options opts [ Aliases ] :aliases A list of aliases.
        #
        # @since 3.0.0
        def initialize(files, chunks, key, opts={})
          @files    = files
          @chunks   = chunks
          @filename = key

          raise GridError, "No matching file found" unless init_grid_reader(opts)

          @file_position = 0
          @current_chunk = next_chunk
          @open          = true
        end

        # Read from the file. If 'length' is specified, will read 'length' characters
        # starting from the current file position. If 'length' is not specified, will
        # read from the current file position until eof.
        #
        # @example Read in 12 characters of data starting from current file position.
        #   f.read(12)
        #
        # @param [ Integer ] length The number of characters to read.
        #
        # @return [ String ] file data.
        #
        # @since 3.0.0
        def read(length=nil)
          opened do
            length ||= @file_length - @file_position
            read_string(length)
          end
        end
        alias :data :read

        # Read a chunk-sized section of data from the file and yield it to the given
        # block. This method begins reading at the current file position.
        #
        # @yield Yields a chunk per iteration as defined by this file's chunk size.
        #
        # @return [ GridReader ] self
        #
        # @since 3.0.0
        def each
          opened do
            return to_enum(:each) unless block_given?
            until eof?
              yield read_string(@chunk_size)
            end
          end
          self
        end

        # Read in the entire file, from the beginning.
        #
        # @return [ String ] file data.
        #
        # @since 3.0.0
        def read_all
          opened do
            seek(0)
            read_string(@file_length)
          end
        end

        # Read from the file until we reach an occurrence of the given character or
        # eof. This begins reading at the current file position.
        #
        # @param [ String ] character The character to match.
        #
        # @return [ String ] file data.
        #
        # @since 3.0.0
        def read_to_character(character='\n')
          opened do
            buf = ''
            while char = getc
              buf << char
              break if char == character
            end
            buf.empty? ? nil : buf
          end
        end

        # Read from the file until we reach an occurrence of the given string or eof.
        # This begins reading at the current file position.
        #
        # @param [ String ] pattern The string to match.
        #
        # @return [ String ] file data.
        #
        # @since 3.0.0
        def read_to_string(pattern='\n')
          opened do
            streaming_substring_search(pattern)
          end
        end

        # Set the file position to the provided location.
        #
        # @param [ Integer ] pos Desired new file position.  This can be a negative
        #   number.
        # @param [ Integer ] whence One of IO::SEEK_CUR, IO::SEEK_END, or IO::SEEK_SET.
        #
        # @return [ Integer ] New file position.
        #
        # @since 3.0.0
        def seek(pos, whence=IO::SEEK_SET)
          opened do
            if whence == IO::SEEK_CUR
              @file_position += pos
            elsif whence == IO::SEEK_END
              @file_position = @file_length + pos
            else
              @file_position = pos
            end
            @file_position
          end
        end

        # Reset the file position to 0.
        #
        # @return [ Integer ] 0.
        #
        # @since 3.0.0
        def rewind
          opened do
            seek(0)
          end
        end

        # Return the current file position.
        #
        # @return [ Integer ] Current file position.
        #
        # @since 3.0.0
        def tell
          opened do
            @file_position
          end
        end
        alias :pos :tell

        # Is the current file pointer at the end of the file?
        #
        # @return [ Boolean ] Eof?
        #
        # @since 3.0.0
        def eof?
          opened do
            @file_position >= @file_length
          end
        end
        alias :eof :eof?

        # Read one character from the file, starting from the current file position.
        #
        # @return [ String ] One character of data from the file.
        #
        # @since 3.0.0
        def getc
          opened do
            read_string(1)
          end
        end

        # Read the next line from the file.
        #
        # @return [ String ] Data from the file.
        #
        # @since 3.0.0
        def gets
          opened do
            read_to_character('\n')
          end
        end

        # Close this GridReader object, disallow further operations on it.  Calling
        # close more than once is allowed.
        #
        # @return [ Boolean ] Closed?
        #
        # @since 3.0.0
        def close
          @open = false
        end

        # Is this file open?
        #
        # @return [ Boolean ] Open?
        #
        # @since 3.0.0
        def open?
          @open
        end

        # Is this file closed?
        #
        # @return [ Boolean ] Closed?
        #
        # @since 3.0.0
        def closed?
          !open?
        end

        # Return a human-readable representation of this GridReader object.
        #
        # @return [ String ] Representation of object.
        #
        # @since 3.0.0
        def inspect
          "#<Mongo::Grid::GridReader:0x#{object_id} @files_id=#{@files_id} @filename=#{@filename}>"
        end

        private

        # Raise an error if this file is closed.
        #
        # @since 3.0.0
        def opened
          raise GridError, "GridReader is closed" unless open?
          yield
        end

        # Find the files document for this file, if one exists, and initialize.
        #
        # @param [ Hash ] opts Options for this file.
        #
        # @options opts [ String ] :fs_name Custom file system prefix.
        # @options opts [ Array ] :aliases A list of String aliases.
        #
        # @since 3.0.0
        def init_grid_reader(opts={})
          return nil unless @files_doc = find_files_doc(opts[:aliases])
          @chunk_size  = @files_doc['chunkSize']
          @files_id    = @files_doc['_id']
          @file_length = @files_doc['length']
        end

        # Find the files document for this file, if one exists.
        #
        # @param [ Array ] aliases An array of String aliases for this file.
        #
        # @return [ Hash ] metadata document.
        #
        # @since 3.0.0
        def find_files_doc(aliases=[])
          # @todo db replace strings with symbols
          if @filename.is_a?(String)
            return @files.find_one({ 'filename' => @filename })
          else
            return @files.find_one({ '_id' => @filename })
          end
          # @todo: pending db and alias spec
          #@files_doc = @files.find_one({ "$or" => [{:filename => @filename },
          #                                         {:aliases => @filename}]})
          #if !@files_doc
          #  aliases.find do | name |
          #    @files_doc = @files.find_one({ "$or" => [{:filename => name},
          #                                             {:aliases => name}]})
          #  end
          # @files_doc
          # end
        end

        # Read a string of data from the file's chunks.
        #
        # @param [ Integer ] length Number of characters to read.
        #
        # @return [ String ] file data.
        #
        # @since 3.0.0
        def read_string(length)
          remaining  = @file_length - @file_position
          length     = length > remaining ? remaining : length
          bytes_read = 0
          buf        = ''

          while bytes_read < length
            chunk = next_chunk
            break unless chunk

            chunk_offset = @file_position % @chunk_size
            to_read = @chunk_size - chunk_offset
            if to_read > (length - bytes_read)
              to_read = (length - bytes_read)
            end

            buf << chunk['data'][chunk_offset, to_read]
            bytes_read += to_read
            @file_position += to_read
          end
          buf.empty? ? nil : buf
        end

        # Get the next chunk from the database, or from our cache.
        #
        # @return [ Hash ] chunk.
        #
        # @since 3.0.0
        def next_chunk
          n = (@file_position / @chunk_size).floor
          if @current_chunk
            if @current_chunk['n'] == n
              return @current_chunk
            end
          end
          @current_chunk = @chunks.find_one({ 'files_id' => @files_id, 'n' => n })
        end

        # Read from the file until an occurrence of 'pattern', or eof.
        #
        # @param [ String ] pattern
        #
        # @return [ String ] data
        #
        # @since 3.0.0
        def streaming_substring_search(pattern)
          morris_pratt_match(pattern)
        end

        # Pre-processing step for Morris-Pratt substring search algorithm.
        #
        # @param [ String ] pattern
        #
        # @return [ Array ] computed offset values for algorithm
        #
        # @since 3.0.0
        def morris_pratt_preprocess(pattern)
          mp_next = Array.new(pattern.length, -1)
          i = 0
          j = -1
          while i < pattern.length
            while j > -1 && pattern[i] != pattern[j]
              j = mp_next[j]
            end
            i += 1
            j += 1
            mp_next[i] = j
          end
          mp_next
        end

        # An implementation of the Morris-Pratt substring search algorithm,
        # modified to read up to the first match in a streaming file.
        # http://www-igm.univ-mlv.fr/~lecroq/string/node7.html
        #
        # @param [ String ] ('\n') pattern Pattern to match
        #
        # @return [ String ] matching data
        #
        # @since 3.0.0
        def morris_pratt_match(pattern='\n')
          return nil unless text = read_string(@chunk_size)

          mp_next = morris_pratt_preprocess(pattern)
          i = 0
          j = 0

          while i < @file_length
            if j >= pattern.length
              return text[0, i]
            end

            if i >= text.length
              more_text = read_string(@chunk_size)
              return nil unless more_text
              text << more_text
            end

            if text[i] == pattern[j]
              i += 1
              j += 1
            else
              i -= mp_next[j]
              j = 0
            end
          end
          text
        end
      end
    end
  end
end
