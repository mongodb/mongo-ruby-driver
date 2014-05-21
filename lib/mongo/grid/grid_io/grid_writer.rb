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

require 'digest'
require 'mime/types'

module Mongo
  module Grid
    module GridIO

      # A file-like object with methods to support the 'w' mode of the GridFileStore
      # and GridFileSystem classes.
      #
      # @note In keeping with the 'w' mode for the Ruby IO class, this class will zero
      # out any existing file with the same files_id and write over it.
      #
      # @since 3.0.0
      class GridWriter

        # @return [ BSON::ObjectId ] Unique identifier for this file.
        attr_reader :files_id
        # @return [ Integer ] Chunk size for this file.
        attr_reader :chunk_size
        # @return [ Integer ] Current file position.
        attr_reader :file_position
        # @return [ Hash ] Metadata for this file.
        attr_reader :files_doc

        # Create a new GridWriter object.
        #
        # @note Users should not need to instantiate this class directly.
        #
        # @param [ Mongo::Collection ] files A collection for storing file metadata.
        # @param [ Mongo::Collection ] chunks A collection for storing chunks of data.
        # @param [ BSON::ObjectID, String ] key The name of this file, or a files_id
        #   for it. If an ObjectId is used, then this file must already exist in the
        #   system or an error will be raised. If an ObjectId is used, this document
        #   will write over the old file and re-use its files_id. If a filename is
        #   used, a new document with a new, unique files_id will be created.
        # @param [ Hash ] opts Options for thie GridWriter instance.
        #
        # @options opts [ Integer ] (261120) :chunk_size Custom chunk size, in bytes.
        # @options opts [ BSON::ObjectId ] :_id Custom ObjectId for the files_id.
        # @options opts [ String ] :fs_name Custom file system prefix
        # @options opts [ Array ] :aliases Array of alias strings for this filename.
        # @options opts [ String ] :content_type A valid MIME type for this document.
        # @options opts [ Hash ] :metadata Any additional metadata to store.
        #
        # @since 3.0.0
        def initialize(files, chunks, key, opts={})
          @files         = files
          @chunks        = chunks
          @file_position = 0
          @local_md5     = Digest::MD5.new
          @write_concern = true # @todo: fix this.
          @files_doc     = init_files_doc(key, opts)

          update_metadata

          @current_chunk = create_chunk(0)
          @open = true
        end

        # Write data to the file, beginning at the current eof.
        #
        # @param [ String, IO ] io Data to write.
        #
        # @return [ Integer ] The number of bytes written.
        #
        # @since 3.0.0
        def write(io)
          opened do
            bytes_written = 0

            if io.is_a?(String)
              bytes_written = write_string(io)
            else
              while msg = io.read(@chunk_size)
                bytes_written += write_string(msg)
              end
            end

            validate_write if @write_concern
            bytes_written
          end
        end

        # Close this writer, no more writes will be allowed.  Calling close
        # multiple times is allowed.
        #
        # @return [ Boolean ] Closed?
        #
        # @since 3.0.0
        def close
          @open = false
        end

        # Is this writer open?
        #
        # @return [ Boolean ] Open?
        #
        # @since 3.0.0
        def open?
          @open
        end

        # Is this writer closed?
        #
        # @return [ Boolean ] Closed?
        #
        # @since 3.0.0
        def closed?
          !open?
        end

        # Return a human-readable representation of this GridWriter object.
        #
        # @return [ String ] Representation of object.
        #
        # @since 3.0.0
        def inspect
          "#<Mongo::Grid::GridWriter:0x#{object_id} @files_id=#{@files_id} @filename=#{@filename}>"
        end

        private

        # Raise an error if this file is closed.
        #
        # @since 3.0.0
        def opened
          raise GridError, "GridWriter is closed" unless open?
          yield
        end

        # Find the matching files document, remove all of its chunks from the 'chunks'
        # collection, remove and replace its metadata.
        #
        # @param[ BSON::ObjectId ] id The files_id.
        #
        # @return [ Hash ] metadata document.
        #
        # @since 3.0.0
        def overwrite(id)
          # @todo db replace strings with symbols
          old_entry = @files.find_one({ '_id' => id })
          if old_entry
            @files.remove({ '_id' => old_entry['_id'] })
            @chunks.remove({ 'files_id' => old_entry['_id'] })
          end
          old_entry
        end

        # Open a file by files_id, reset its state, and return its files document.
        #
        # @param [ BSON::ObjectId ] id The files_id.
        #
        # @return [ Hash ] metadata document.
        #
        # @since 3.0.0
        def open_by_id(id)
          file = overwrite(id)
          raise GridError, "Cannot open a new file with an ObjectId #{id}" unless file

          @files_id          = file['_id']
          @filename          = file['filename']
          file['length']     = 0
          file['uploadDate'] = Time.now.utc
          file
        end

        # Given a key and options, open and initialize existing metadata document,
        # or create a new one.
        #
        # @param [ String, BSON::ObjectId ] key Filename or files_id.
        # @param [ Hash ] opts Options.
        #
        # @options opts [ Integer ] (261120) :chunk_size Custom chunk size, in bytes.
        # @options opts [ BSON::ObjectId ] :_id Custom ObjectId for the files_id.
        # @options opts [ String ] :fs_name Custom file system prefix
        # @options opts [ Array ] :aliases Array of alias strings for this filename.
        # @options opts [ String ] :content_type A valid MIME type for this document.
        # @options opts [ Hash ] :metadata Any additional metadata to store.
        #
        # @return [ Hash ] metadata document.
        #
        # @since 3.0.0
        def init_files_doc(key, opts={})
          @fs_name = opts[:fs_name]       || DEFAULT_FS_NAME
          @chunk_size = opts[:chunk_size] || DEFAULT_CHUNK_SIZE

          return open_by_id(key) if key.is_a?(BSON::ObjectId)

          @filename = key
          @files_id = opts[:_id] || BSON::ObjectId.new

          # @todo db replace strings with symbols
          { '_id'         => @files_id,
            'chunkSize'   => @chunk_size,
            'filename'    => @filename,
            'md5'         => @local_md5,
            'length'      => 0,
            'uploadDate'  => Time.now.utc,
            'contentType' => content_type(opts[:content_type]),
            'aliases'     => opts[:aliases] || [],
            'metadata'    => opts[:metadata] || {} }
        end

        # If 'mime' is a valid MIME type, return it.
        # Or, if we can guess this file's MIME type from its filename, return that.
        # Otherwise, return the default MIME type.
        #
        # @param [ String ] mime A potential MIME type.
        #
        # @return [ String ] simplified MIME type.
        #
        # @since 3.0.0
        def content_type(mime)
          if types = MIME::Types[mime]
            return types.first.simplified unless types.empty?
          end
          if types = MIME::Types.type_for(@filename)
            return MIME::Type.simplified(types.first) unless types.empty?
          end
          DEFAULT_CONTENT_TYPE
        end

        # Write 'msg' to the file store.
        #
        # @param [ String ] msg Data to write.
        #
        # @return [ Integer ] number of characters written.
        #
        # @since 3.0.0
        def write_string(msg)
          bytes_written = 0
          while bytes_written < msg.bytesize
            chunk = next_chunk
            free_space = @chunk_size - (@file_position % @chunk_size)
            bytes_left = msg.bytesize - bytes_written
            to_write = bytes_left <= free_space ? bytes_left : free_space

            chunk['data'] << msg[bytes_written, to_write]
            bytes_written += to_write
            @file_position += to_write
            save_chunk(chunk)
          end
          @local_md5.update(msg)
          bytes_written
        end

        # Validate that a write went through to the database.
        #
        # @since 3.0.0
        def validate_write
          # @todo db
          #        @server_md5 = @files.db.command({ :filemd5 => @files_id,
          #                                          :root => @fs_name })
          #        raise GridMD5Failure, "Failed MD5 check" unless @local_md5 == @server_md5
        end

        # Create a new, empty chunk at index 'n'.
        #
        # @param [ Integer ] n Index of this chunk.
        #
        # @return [ Hash ] a new chunk.
        #
        # @since 3.0.0
        def create_chunk(n)
          # @todo db replace strings with symbols
          { '_id'      => BSON::ObjectId.new,
            'files_id' => @files_id,
            'n'        => n,
            'data'     => '' }
        end

        # Save this chunk to the chunks collection and update metadata for this file.
        #
        # @param [ Hash ] chunk Chunk to be saved.
        #
        # @since 3.0.0
        def save_chunk(chunk)
          @chunks.save(chunk)
          update_metadata
        end

        # Update the metadata for this document and save it to the 'files' collection.
        #
        # @since 3.0.0
        def update_metadata
          @files_doc['length'] = @file_position
          @files_doc['md5'] = @local_md5
          @files.save(@files_doc)
        end

        # Get the next chunk from the database, or from our cache if we have cached it.
        #
        # @return [ Hash ] chunk.
        #
        # @since 3.0.0
        def next_chunk
          n = (@file_position / @chunk_size).floor
          if @current_chunk['n'] != n
            chunk_offset = @file_position % @chunk_size
            if chunk_offset == 0
              @current_chunk = create_chunk(n)
            else
              @current_chunk = @chunks.find_one({ :files_id => @files_id,
                                                  :n => n })
            end
          end
          @current_chunk
        end
      end
    end
  end
end
