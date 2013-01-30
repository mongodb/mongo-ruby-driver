module Mongo

  # A file store built on the GridFS specification featuring
  # an API and behavior similar to that of a traditional file system.
  class GridFileSystem
    include GridExt::InstanceMethods

    # Initialize a new GridFileSystem instance, consisting of a MongoDB database
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

      @default_query_opts = {:sort => [['filename', 1], ['uploadDate', -1]], :limit => 1}

      # This will create indexes only if we're connected to a primary node.
      connection = @db.connection
      begin
        @files.ensure_index([['filename', 1], ['uploadDate', -1]])
        @chunks.ensure_index([['files_id', Mongo::ASCENDING], ['n', Mongo::ASCENDING]], :unique => true)
      rescue Mongo::ConnectionFailure
      end
    end

    # Open a file for reading or writing. Note that the options for this method only apply
    # when opening in 'w' mode.
    #
    # Note that arbitrary metadata attributes can be saved to the file by passing
    # them is as options.
    #
    # @param [String] filename the name of the file.
    # @param [String] mode either 'r' or 'w' for reading from
    #   or writing to the file.
    # @param [Hash] opts see GridIO#new
    #
    # @option opts [Hash] :metadata ({}) any additional data to store with the file.
    # @option opts [ObjectId] :_id (ObjectId) a unique id for
    #   the file to be use in lieu of an automatically generated one.
    # @option opts [String] :content_type ('binary/octet-stream') If no content type is specified,
    #   the content type will may be inferred from the filename extension if the mime-types gem can be
    #   loaded. Otherwise, the content type 'binary/octet-stream' will be used.
    # @option opts [Integer] (262144) :chunk_size size of file chunks in bytes.
    # @option opts [Boolean] :delete_old (false) ensure that old versions of the file are deleted. This option
    #  only work in 'w' mode. Certain precautions must be taken when deleting GridFS files. See the notes under
    #  GridFileSystem#delete.
    # @option opts [String, Integer, Symbol] :w (1) Set write concern
    #
    #   Notes on write concern:
    #     When :w > 0, the chunks sent to the server
    #     will be validated using an md5 hash. If validation fails, an exception will be raised.
    # @option opts [Integer] :versions (false) deletes all versions which exceed the number specified to 
    #   retain ordered by uploadDate. This option only works in 'w' mode. Certain precautions must be taken when 
    #   deleting GridFS files. See the notes under GridFileSystem#delete.
    #
    # @example
    #
    #  # Store the text "Hello, world!" in the grid file system.
    #  @grid = Mongo::GridFileSystem.new(@db)
    #  @grid.open('filename', 'w') do |f|
    #    f.write "Hello, world!"
    #  end
    #
    #  # Output "Hello, world!"
    #  @grid = Mongo::GridFileSystem.new(@db)
    #  @grid.open('filename', 'r') do |f|
    #    puts f.read
    #  end
    #
    #  # Write a file on disk to the GridFileSystem
    #  @file = File.open('image.jpg')
    #  @grid = Mongo::GridFileSystem.new(@db)
    #  @grid.open('image.jpg, 'w') do |f|
    #    f.write @file
    #  end
    #
    #  @return [Mongo::GridIO]
    def open(filename, mode, opts={})
      opts = opts.dup
      opts.merge!(default_grid_io_opts(filename))
      if mode == 'w'
        begin
          # Ensure there are the appropriate indexes, as state may have changed since instantiation of self.
          # Recall that index definitions are cached with ensure_index so this statement won't unneccesarily repeat index creation.
          @files.ensure_index([['filename', 1], ['uploadDate', -1]])
          @chunks.ensure_index([['files_id', Mongo::ASCENDING], ['n', Mongo::ASCENDING]], :unique => true)
          versions = opts.delete(:versions)
          if opts.delete(:delete_old) || (versions && versions < 1)
            versions = 1
          end
        rescue Mongo::ConnectionFailure => e
          raise e, "Failed to create necessary indexes and write data."
          return
        end
      end
      file = GridIO.new(@files, @chunks, filename, mode, opts)
      return file unless block_given?
      result = nil
      begin
        result = yield file
      ensure
        id = file.close
        if versions
          self.delete do
            @files.find({'filename' => filename, '_id' => {'$ne' => id}}, :fields => ['_id'], :sort => ['uploadDate', -1], :skip => (versions - 1))
          end
        end
      end
      result
    end

    # Delete the file with the given filename. Note that this will delete
    # all versions of the file.
    #
    # Be careful with this. Deleting a GridFS file can result in read errors if another process
    # is attempting to read a file while it's being deleted. While the odds for this
    # kind of race condition are small, it's important to be aware of.
    #
    # @param [String] filename
    #
    # @yield [] pass a block that returns an array of documents to be deleted.
    #
    # @return [Boolean]
    def delete(filename=nil)
      if block_given?
        files = yield
      else
        files = @files.find({'filename' => filename}, :fields => ['_id'])
      end
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
