# --
# Copyright (C) 2008-2010 10gen Inc.
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

  # A file store built on the GridFS specification featuring
  # an API and behavior similar to that of a traditional file system.
  class GridFileSystem

    # Initialize a new Grid instance, consisting of a MongoDB database
    # and a filesystem prefix if not using the default.
    #
    # @param [Mongo::DB] db a MongoDB database.
    # @param [String] fs_name A name for the file system. The default name, based on
    #   the specification, is 'fs'.
    def initialize(db, fs_name=Grid::DEFAULT_FS_NAME)
      raise MongoArgumentError, "db must be a Mongo::DB." unless db.is_a?(Mongo::DB)

      @db      = db
      @files   = @db["#{fs_name}.files"]
      @chunks  = @db["#{fs_name}.chunks"]
      @fs_name = fs_name

      @files.create_index([['filename', 1], ['uploadDate', -1]])
      @default_query_opts = {:sort => [['filename', 1], ['uploadDate', -1]], :limit => 1}
    end

    # Open a file for reading or writing. Note that the options for this method only apply
    # when opening in 'w' mode.
    #
    # @param [String] filename the name of the file.
    # @param [String] mode either 'r' or 'w' for reading from
    #   or writing to the file.
    # @param [Hash] opts see GridIO#new
    #
    # @options opts [Hash] :metadata ({}) any additional data to store with the file.
    # @options opts [ObjectID] :_id (ObjectID) a unique id for
    #   the file to be use in lieu of an automatically generated one.
    # @options opts [String] :content_type ('binary/octet-stream') If no content type is specified,
    #   the content type will may be inferred from the filename extension if the mime-types gem can be
    #   loaded. Otherwise, the content type 'binary/octet-stream' will be used.
    # @options opts [Integer] (262144) :chunk_size size of file chunks in bytes.
    # @options opts [Boolean] :safe (false) When safe mode is enabled, the chunks sent to the server
    #   will be validated using an md5 hash. If validation fails, an exception will be raised.
    #
    # @example
    #
    #  # Store the text "Hello, world!" in the grid file system.
    #  @grid = GridFileSystem.new(@db)
    #  @grid.open('filename', 'w') do |f|
    #    f.write "Hello, world!"
    #  end
    #
    #  # Output "Hello, world!"
    #  @grid = GridFileSystem.new(@db)
    #  @grid.open('filename', 'r') do |f|
    #    puts f.read
    #  end
    #
    #  # Write a file on disk to the GridFileSystem
    #  @file = File.open('image.jpg')
    #  @grid = GridFileSystem.new(@db)
    #  @grid.open('image.jpg, 'w') do |f|
    #    f.write @file
    #  end
    def open(filename, mode, opts={})
      opts.merge!(default_grid_io_opts(filename))
      file   = GridIO.new(@files, @chunks, filename, mode, opts)
      return file unless block_given?
      result = nil
      begin
        result = yield file
      ensure
        file.close
      end
      result
    end

    # Delete the file with the given filename. Note that this will delete
    # all versions of the file.
    #
    # @param [String] filename
    #
    # @return [Boolean]
    def delete(filename)
      files = @files.find({'filename' => filename}, :fields => ['_id'])
      files.each do |file|
        @files.remove({'_id' => file['_id']})
        @chunks.remove({'files_id' => file['_id']})
      end
    end
    alias_method :unlink, :delete

    private

    def default_grid_io_opts(filename=nil)
      {:fs_name => @fs_name, :query => {'filename' => filename}, :query_opts => @default_query_opts}
    end
  end
end
