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

  # Implementation of the MongoDB GridFS specification. A file store.
  class Grid
    DEFAULT_FS_NAME = 'fs'

    # Initialize a new Grid instance, consisting of a MongoDB database
    # and a filesystem prefix if not using the default.
    #
    # @core gridfs
    #
    # @see GridFileSystem
    def initialize(db, fs_name=DEFAULT_FS_NAME)
      raise MongoArgumentError, "db must be a Mongo::DB." unless db.is_a?(Mongo::DB)

      @db      = db
      @files   = @db["#{fs_name}.files"]
      @chunks  = @db["#{fs_name}.chunks"]
      @fs_name = fs_name

      @chunks.create_index([['files_id', Mongo::ASCENDING], ['n', Mongo::ASCENDING]])
    end

    # Store a file in the file store.
    #
    # @param [String, #read] data a string or io-like object to store.
    # @param [String] filename a name for the file.
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
    # @return [Mongo::ObjectID] the file's id.
    def put(data, filename, opts={})
      opts.merge!(default_grid_io_opts)
      file = GridIO.new(@files, @chunks, filename, 'w', opts=opts)
      file.write(data)
      file.close
      file.files_id
    end

    # Read a file from the file store.
    #
    # @param [] id the file's unique id.
    #
    # @return [Mongo::GridIO]
    def get(id)
      opts = {:query => {'_id' => id}}.merge!(default_grid_io_opts)
      GridIO.new(@files, @chunks, nil, 'r', opts)
    end

    # Delete a file from the store.
    #
    # @param [] id
    #
    # @return [Boolean]
    def delete(id)
      @files.remove({"_id" => id})
      @chunks.remove({"_id" => id})
    end

    private

    def default_grid_io_opts
      {:fs_name => @fs_name}
    end
  end
end
