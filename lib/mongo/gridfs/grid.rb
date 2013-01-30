module Mongo

  # Implementation of the MongoDB GridFS specification. A file store.
  class Grid
    include GridExt::InstanceMethods

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

      # This will create indexes only if we're connected to a primary node.
      connection = @db.connection
      begin
        @chunks.ensure_index([['files_id', Mongo::ASCENDING], ['n', Mongo::ASCENDING]], :unique => true)
      rescue Mongo::ConnectionFailure
      end
    end

    # Store a file in the file store. This method is designed only for writing new files;
    # if you need to update a given file, first delete it using Grid#delete.
    #
    # Note that arbitrary metadata attributes can be saved to the file by passing
    # them in as options.
    #
    # @param [String, #read] data a string or io-like object to store.
    #
    # @option opts [String] :filename (nil) a name for the file.
    # @option opts [Hash] :metadata ({}) any additional data to store with the file.
    # @option opts [ObjectId] :_id (ObjectId) a unique id for
    #   the file to be use in lieu of an automatically generated one.
    # @option opts [String] :content_type ('binary/octet-stream') If no content type is specified,
    #   the content type will may be inferred from the filename extension if the mime-types gem can be
    #   loaded. Otherwise, the content type 'binary/octet-stream' will be used.
    # @option opts [Integer] (262144) :chunk_size size of file chunks in bytes.
    # @option opts [String, Integer, Symbol] :w (1) Set write concern
    #
    #   Notes on write concern:  
    #      When :w > 0, the chunks sent to the server are validated using an md5 hash. 
    #      If validation fails, an exception will be raised.
    #
    # @return [BSON::ObjectId] the file's id.
    def put(data, opts={})
      begin
        # Ensure there is an index on files_id and n, as state may have changed since instantiation of self.
        # Recall that index definitions are cached with ensure_index so this statement won't unneccesarily repeat index creation.
        @chunks.ensure_index([['files_id', Mongo::ASCENDING], ['n', Mongo::ASCENDING]], :unique => true)
        opts     = opts.dup
        filename = opts.delete(:filename)
        opts.merge!(default_grid_io_opts)
        file = GridIO.new(@files, @chunks, filename, 'w', opts)
        file.write(data)
        file.close
        file.files_id
      rescue Mongo::ConnectionFailure => e
        raise e, "Failed to create necessary index and write data."
      end
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
    # Note that deleting a GridFS file can result in read errors if another process
    # is attempting to read a file while it's being deleted. While the odds for this
    # kind of race condition are small, it's important to be aware of.
    #
    # @param [] id
    #
    # @return [Boolean]
    def delete(id)
      @files.remove({"_id" => id})
      @chunks.remove({"files_id" => id})
    end

    private

    def default_grid_io_opts
      {:fs_name => @fs_name}
    end
  end
end
