# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

module Mongo

  # WARNING: This is part of a new, experimental GridFS API. Subject to change.
  class GridIO
    DEFAULT_CHUNK_SIZE   = 256 * 1024
    DEFAULT_CONTENT_TYPE = 'binary/octet-stream'

    attr_reader :content_type, :chunk_size, :upload_date, :files_id, :filename, :metadata

    def initialize(files, chunks, filename, mode, filesystem, opts={})
      @files    = files
      @chunks   = chunks
      @filename = filename
      @mode     = mode
      @content_type = opts[:content_type] || DEFAULT_CONTENT_TYPE
      @chunk_size   = opts[:chunk_size] || DEFAULT_CHUNK_SIZE
      @files_id     = opts[:_id]

      case @mode
        when 'r' then init_read(filesystem, opts)
        when 'w' then init_write(opts)
        else
          raise GridError, "Invalid file mode #{@mode}. Valid options include 'r' and 'w'."
      end
    end

    # Read the data from the file. If a length if specified, will read from the
    # current file position.
    #
    # @param [Integer] length
    #
    # @return [String]
    #   the data in the file
    def read(length=nil)
      return '' if length == 0
      return read_all if length.nil? && @file_position.zero?
      buf = ''
      while true
        buf << @current_chunk['data'].to_s[@chunk_position..-1]
        if buf.length >= length
          return buf[0...length]
        else
          @current_chunk = get_chunk(@current_chunk['n'] + 1)
        end
      end
      buf
    end
    alias :data :read

    # Write the given string (binary) data to the file.
    #
    # @param [String] string
    #   the data to write
    #
    # @return [Integer]
    #   the number of bytes written.
    def write(string)
      raise GridError, "#{@filename} not opened for write" unless @mode[0] == ?w
      # Since Ruby 1.9.1 doesn't necessarily store one character per byte.
      if string.respond_to?(:force_encoding)
        string.force_encoding("binary")
      end
      to_write = string.length
      while (to_write > 0) do
        if @current_chunk && @chunk_position == @chunk_size
          next_chunk_number = @current_chunk['n'] + 1
          @current_chunk    = create_chunk(next_chunk_number)
        end
        chunk_available = @chunk_size - @chunk_position
        step_size = (to_write > chunk_available) ? chunk_available : to_write
        @current_chunk['data'] = Binary.new((@current_chunk['data'].to_s << string[-to_write, step_size]).unpack("c*"))
        @chunk_position += step_size
        to_write -= step_size
        save_chunk(@current_chunk)
      end
      string.length - to_write
    end

    # Position the file pointer at the provided location.
    #
    # @param [Integer] pos
    #   the number of bytes to advance the file pointer. this can be a negative
    #   number.
    # @param [Integer] whence
    #   one of IO::SEEK_CUR, IO::SEEK_END, or IO::SEEK_SET
    #
    # @return [Integer] the new file position
    def seek(pos, whence=IO::SEEK_SET)
      raise GridError, "Seek is only allowed in read mode." unless @mode == 'r'
      target_pos = case whence
                   when IO::SEEK_CUR
                     @file_position + pos
                   when IO::SEEK_END
                     @file_length + pos
                   when IO::SEEK_SET
                     pos
                   end

      new_chunk_number = (target_pos / @chunk_size).to_i
      if new_chunk_number != @current_chunk['n']
        save_chunk(@current_chunk) if @mode[0] == ?w
        @current_chunk = get_chunk(new_chunk_number)
      end
      @file_position  = target_pos
      @chunk_position = @file_position % @chunk_size
      @file_position
    end

    # The current position of the file.
    #
    # @return [Integer]
    def tell
      @file_position
    end

    # Creates or updates the document storing the chunks' metadata
    # in the files collection. The file exists only after this method
    # is called.
    #
    # This method will be invoked automatically
    # on GridIO#open. Otherwise, it must be called manually.
    #
    # @return [True]
    def close
      if @mode[0] == ?w
        @upload_date = Time.now.utc
        @files.insert(to_mongo_object)
      end
      true
    end

    def inspect
      "_id: #{@files_id}"
    end

    private

    def create_chunk(n)
      chunk = OrderedHash.new
      chunk['_id']      = Mongo::ObjectID.new
      chunk['n']        = n
      chunk['files_id'] = @files_id
      chunk['data']     = ''
      @chunk_position   = 0
      chunk
    end

    # TODO: Perhaps use an upsert here instead?
    def save_chunk(chunk)
      @chunks.remove('_id' => chunk['_id'])
      @chunks.insert(chunk)
    end

    def get_chunk(n)
      chunk = @chunks.find({'files_id' => @files_id, 'n' => n}).next_document
      @chunk_position = 0
      chunk || {}
    end

    def last_chunk_number
      (@file_length / @chunk_size).to_i
    end

    # An optimized read method for reading the whole file.
    def read_all
      buf = ''
      while true
        buf << @current_chunk['data'].to_s
        break if @current_chunk['n'] == last_chunk_number
        @current_chunk = get_chunk(@current_chunk['n'] + 1)
      end
      buf
    end

    # Initialize based on whether the supplied file exists.
    def init_read(filesystem, opts)
      if filesystem
        doc = @files.find({'filename' => @filename}, :sort => [["uploadDate", -1]], :limit => 1).next_document
        raise GridError, "Could not open file with filename #{@filename}" unless doc
      else
        doc = @files.find({'_id' => @files_id}).next_document
        raise GridError, "Could not open file with id #{@files_id}" unless doc
      end

      @files_id     = doc['_id']
      @content_type = doc['contentType']
      @chunk_size   = doc['chunkSize']
      @upload_date  = doc['uploadDate']
      @aliases      = doc['aliases']
      @file_length  = doc['length']
      @metadata     = doc['metadata']
      @md5          = doc['md5']
      @filename     = doc['filename']
      @current_chunk = get_chunk(0)
      @file_position = 0
    end

    # Validates and sets up the class for the given file mode.
    def init_write(opts)
      @files_id      = opts[:_id] || Mongo::ObjectID.new
      @content_type  = opts[:content_type] || @content_type || DEFAULT_CONTENT_TYPE
      @chunk_size    = opts[:chunk_size]   || @chunk_size || DEFAULT_CHUNK_SIZE
      @file_length   = 0
      @metadata      = opts[:metadata] if opts[:metadata]

      @current_chunk = create_chunk(0)
      @file_position = 0
    end

    def to_mongo_object
      h                = OrderedHash.new
      h['_id']         = @files_id
      h['filename']    = @filename
      h['contentType'] = @content_type
      h['length']      = @current_chunk ? @current_chunk['n'] * @chunk_size + @chunk_position : 0
      h['chunkSize']   = @chunk_size
      h['uploadDate']  = @upload_date
      h['aliases']     = @aliases
      h['metadata']    = @metadata

      # Get a server-side md5.
      md5_command            = OrderedHash.new
      md5_command['filemd5'] = @files_id
      md5_command['root']    = 'fs'
      h['md5']               = @files.db.command(md5_command)['md5']

      h
    end
  end
end
