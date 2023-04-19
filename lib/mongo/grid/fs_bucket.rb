# frozen_string_literal: true
# rubocop:todo all

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

    # Represents a view of the GridFS in the database.
    #
    # @since 2.0.0
    class FSBucket
      extend Forwardable

      # The default root prefix.
      #
      # @since 2.0.0
      DEFAULT_ROOT = 'fs'.freeze

      # The specification for the chunks collection index.
      #
      # @since 2.0.0
      CHUNKS_INDEX = { :files_id => 1, :n => 1 }.freeze

      # The specification for the files collection index.
      #
      # @since 2.1.0
      FILES_INDEX = { filename: 1, uploadDate: 1 }.freeze

      # Create the GridFS.
      #
      # @example Create the GridFS.
      #   Grid::FSBucket.new(database)
      #
      # @param [ Database ] database The database the files reside in.
      # @param [ Hash ] options The GridFS options.
      #
      # @option options [ String ] :bucket_name The prefix for the files and chunks
      #   collections.
      # @option options [ Integer ] :chunk_size Override the default chunk
      #   size.
      # @option options [ String ] :fs_name The prefix for the files and chunks
      #   collections.
      # @option options [ Hash ] :read The read preference options. The hash
      #   may have the following items:
      #   - *:mode* -- read preference specified as a symbol; valid values are
      #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
      #     and *:nearest*.
      #   - *:tag_sets* -- an array of hashes.
      #   - *:local_threshold*.
      # @option options [ Session ] :session The session to use.
      # @option options [ Hash ] :write Deprecated. Equivalent to :write_concern
      #   option.
      # @option options [ Hash ] :write_concern The write concern options.
      #   Can be :w => Integer|String, :fsync => Boolean, :j => Boolean.
      #
      # @since 2.0.0
      def initialize(database, options = {})
        @database = database
        @options = options.dup
=begin WriteConcern object support
        if @options[:write_concern].is_a?(WriteConcern::Base)
          # Cache the instance so that we do not needlessly reconstruct it.
          @write_concern = @options[:write_concern]
          @options[:write_concern] = @write_concern.options
        end
=end
        @options.freeze
        @chunks_collection = database[chunks_name]
        @files_collection = database[files_name]
      end

      # @return [ Collection ] chunks_collection The chunks collection.
      #
      # @since 2.0.0
      attr_reader :chunks_collection

      # @return [ Database ] database The database.
      #
      # @since 2.0.0
      attr_reader :database

      # @return [ Collection ] files_collection The files collection.
      #
      # @since 2.0.0
      attr_reader :files_collection

      # @return [ Hash ] options The FSBucket options.
      #
      # @since 2.1.0
      attr_reader :options

      # Get client from the database.
      #
      # @since 2.1.0
      def_delegators :database,
                     :client

      # Find files collection documents matching a given selector.
      #
      # @example Find files collection documents by a filename.
      #   fs.find(filename: 'file.txt')
      #
      # @param [ Hash ] selector The selector to use in the find.
      # @param [ Hash ] options The options for the find.
      #
      # @option options [ true, false ] :allow_disk_use Whether the server can
      #   write temporary data to disk while executing the find operation.
      # @option options [ Integer ] :batch_size The number of documents returned in each batch
      #   of results from MongoDB.
      # @option options [ Integer ] :limit The max number of docs to return from the query.
      # @option options [ true, false ] :no_cursor_timeout The server normally times out idle
      #   cursors after an inactivity period (10 minutes) to prevent excess memory use.
      #   Set this option to prevent that.
      # @option options [ Integer ] :skip The number of docs to skip before returning results.
      # @option options [ Hash ] :sort The key and direction pairs by which the result set
      #   will be sorted.
      #
      # @return [ CollectionView ] The collection view.
      #
      # @since 2.1.0
      def find(selector = nil, options = {})
        opts = options.merge(read: read_preference) if read_preference
        files_collection.find(selector, opts || options)
      end

      # Find a file in the GridFS.
      #
      # @example Find a file by its id.
      #   fs.find_one(_id: id)
      #
      # @example Find a file by its filename.
      #   fs.find_one(filename: 'test.txt')
      #
      # @param [ Hash ] selector The selector.
      #
      # @return [ Grid::File ] The file.
      #
      # @since 2.0.0
      #
      # @deprecated Please use #find instead with a limit of -1.
      #   Will be removed in version 3.0.
      def find_one(selector = nil)
        file_info = files_collection.find(selector).first
        return nil unless file_info
        chunks = chunks_collection.find(:files_id => file_info[:_id]).sort(:n => 1)
        Grid::File.new(chunks.to_a, Options::Mapper.transform(file_info, Grid::File::Info::MAPPINGS.invert))
      end

      # Insert a single file into the GridFS.
      #
      # @example Insert a single file.
      #   fs.insert_one(file)
      #
      # @param [ Grid::File ] file The file to insert.
      #
      # @return [ BSON::ObjectId ] The file id.
      #
      # @since 2.0.0
      #
      # @deprecated Please use #upload_from_stream or #open_upload_stream instead.
      #   Will be removed in version 3.0.
      def insert_one(file)
        @indexes ||= ensure_indexes!
        chunks_collection.insert_many(file.chunks)
        files_collection.insert_one(file.info)
        file.id
      end

      # Get the prefix for the GridFS
      #
      # @example Get the prefix.
      #   fs.prefix
      #
      # @return [ String ] The GridFS prefix.
      #
      # @since 2.0.0
      def prefix
        @options[:fs_name] || @options[:bucket_name] || DEFAULT_ROOT
      end

      # Remove a single file from the GridFS.
      #
      # @example Remove a file from the GridFS.
      #   fs.delete_one(file)
      #
      # @param [ Grid::File ] file The file to remove.
      #
      # @return [ Result ] The result of the remove.
      #
      # @since 2.0.0
      def delete_one(file)
        delete(file.id)
      end

      # Remove a single file, identified by its id from the GridFS.
      #
      # @example Remove a file from the GridFS.
      #   fs.delete(id)
      #
      # @param [ BSON::ObjectId, Object ] id The id of the file to remove.
      #
      # @return [ Result ] The result of the remove.
      #
      # @raise [ Error::FileNotFound ] If the file is not found.
      #
      # @since 2.1.0
      def delete(id)
        result = files_collection.find({ :_id => id }, @options).delete_one
        chunks_collection.find({ :files_id => id }, @options).delete_many
        raise Error::FileNotFound.new(id, :id) if result.n == 0
        result
      end

      # Opens a stream from which a file can be downloaded, specified by id.
      #
      # @example Open a stream from which a file can be downloaded.
      #   fs.open_download_stream(id)
      #
      # @param [ BSON::ObjectId, Object ] id The id of the file to read.
      # @param [ Hash ] options The options.
      #
      # @option options [ BSON::Document ] :file_info_doc For internal
      #   driver use only. A BSON document to use as file information.
      #
      # @return [ Stream::Read ] The stream to read from.
      #
      # @yieldparam [ Hash ] The read stream.
      #
      # @since 2.1.0
      def open_download_stream(id, options = nil)
        options = Utils.shallow_symbolize_keys(options || {})
        read_stream(id, **options).tap do |stream|
          if block_given?
            begin
              yield stream
            ensure
              stream.close
            end
          end
        end
      end

      # Downloads the contents of the file specified by id and writes them to
      # the destination io object.
      #
      # @example Download the file and write it to the io object.
      #   fs.download_to_stream(id, io)
      #
      # @param [ BSON::ObjectId, Object ] id The id of the file to read.
      # @param [ IO ] io The io object to write to.
      #
      # @since 2.1.0
      def download_to_stream(id, io)
        open_download_stream(id) do |stream|
          stream.each do |chunk|
            io << chunk
          end
        end
      end

      # Opens a stream from which the application can read the contents of the stored file
      # specified by filename and the revision in options.
      #
      # Revision numbers are defined as follows:
      # 0 = the original stored file
      # 1 = the first revision
      # 2 = the second revision
      # etc…
      # -2 = the second most recent revision
      # -1 = the most recent revision
      #
      # @example Open a stream to download the most recent revision.
      #   fs.open_download_stream_by_name('some-file.txt')
      #
      # # @example Open a stream to download the original file.
      #   fs.open_download_stream_by_name('some-file.txt', revision: 0)
      #
      # @example Open a stream to download the second revision of the stored file.
      #   fs.open_download_stream_by_name('some-file.txt', revision: 2)
      #
      # @param [ String ] filename The file's name.
      # @param [ Hash ] opts Options for the download.
      #
      # @option opts [ Integer ] :revision The revision number of the file to download.
      #   Defaults to -1, the most recent version.
      #
      # @return [ Stream::Read ] The stream to read from.
      #
      # @raise [ Error::FileNotFound ] If the file is not found.
      # @raise [ Error::InvalidFileRevision ] If the requested revision is not found for the file.
      #
      # @yieldparam [ Hash ] The read stream.
      #
      # @since 2.1.0
      def open_download_stream_by_name(filename, opts = {}, &block)
        revision = opts.fetch(:revision, -1)
        if revision < 0
          skip = revision.abs - 1
          sort = { 'uploadDate' => Mongo::Index::DESCENDING }
        else
          skip = revision
          sort = { 'uploadDate' => Mongo::Index::ASCENDING }
        end
        file_info_doc = files_collection.find({ filename: filename} ,
                                           sort: sort,
                                           skip: skip,
                                           limit: -1).first
        unless file_info_doc
          raise Error::FileNotFound.new(filename, :filename) unless opts[:revision]
          raise Error::InvalidFileRevision.new(filename, opts[:revision])
        end
        open_download_stream(file_info_doc[:_id], file_info_doc: file_info_doc, &block)
      end

      # Downloads the contents of the stored file specified by filename and by the
      # revision in options and writes the contents to the destination io object.
      #
      # Revision numbers are defined as follows:
      # 0 = the original stored file
      # 1 = the first revision
      # 2 = the second revision
      # etc…
      # -2 = the second most recent revision
      # -1 = the most recent revision
      #
      # @example Download the most recent revision.
      #   fs.download_to_stream_by_name('some-file.txt', io)
      #
      # # @example Download the original file.
      #   fs.download_to_stream_by_name('some-file.txt', io, revision: 0)
      #
      # @example Download the second revision of the stored file.
      #   fs.download_to_stream_by_name('some-file.txt', io, revision: 2)
      #
      # @param [ String ] filename The file's name.
      # @param [ IO ] io The io object to write to.
      # @param [ Hash ] opts Options for the download.
      #
      # @option opts [ Integer ] :revision The revision number of the file to download.
      #   Defaults to -1, the most recent version.
      #
      # @raise [ Error::FileNotFound ] If the file is not found.
      # @raise [ Error::InvalidFileRevision ] If the requested revision is not found for the file.
      #
      # @since 2.1.0
      def download_to_stream_by_name(filename, io, opts = {})
        download_to_stream(open_download_stream_by_name(filename, opts).file_id, io)
      end

      # Opens an upload stream to GridFS to which the contents of a file or
      # blob can be written.
      #
      # @param [ String ] filename The name of the file in GridFS.
      # @param [ Hash ] opts The options for the write stream.
      #
      # @option opts [ Object ] :file_id An optional unique file id.
      #   A BSON::ObjectId is automatically generated if a file id is not
      #   provided.
      # @option opts [ Integer ] :chunk_size Override the default chunk size.
      # @option opts [ Hash ] :metadata User data for the 'metadata' field of the files
      #   collection document.
      # @option opts [ String ] :content_type The content type of the file.
      #   Deprecated, please use the metadata document instead.
      # @option opts [ Array<String> ] :aliases A list of aliases.
      #   Deprecated, please use the metadata document instead.
      # @option options [ Hash ] :write Deprecated. Equivalent to :write_concern
      #   option.
      # @option options [ Hash ] :write_concern The write concern options.
      #   Can be :w => Integer|String, :fsync => Boolean, :j => Boolean.
      #
      # @return [ Stream::Write ] The write stream.
      #
      # @yieldparam [ Hash ] The write stream.
      #
      # @since 2.1.0
      def open_upload_stream(filename, opts = {})
        opts = Utils.shallow_symbolize_keys(opts)
        write_stream(filename, **opts).tap do |stream|
          if block_given?
            begin
              yield stream
            ensure
              stream.close
            end
          end
        end
      end

      # Uploads a user file to a GridFS bucket.
      # Reads the contents of the user file from the source stream and uploads it as chunks in the
      # chunks collection. After all the chunks have been uploaded, it creates a files collection
      # document for the filename in the files collection.
      #
      # @example Upload a file to the GridFS bucket.
      #   fs.upload_from_stream('a-file.txt', file)
      #
      # @param [ String ] filename The filename of the file to upload.
      # @param [ IO ] io The source io stream to upload from.
      # @param [ Hash ] opts The options for the write stream.
      #
      # @option opts [ Object ] :file_id An optional unique file id. An ObjectId is generated otherwise.
      # @option opts [ Integer ] :chunk_size Override the default chunk size.
      # @option opts [ Hash ] :metadata User data for the 'metadata' field of the files
      #   collection document.
      # @option opts [ String ] :content_type The content type of the file. Deprecated, please
      #   use the metadata document instead.
      # @option opts [ Array<String> ] :aliases A list of aliases. Deprecated, please use the
      #   metadata document instead.
      # @option options [ Hash ] :write Deprecated. Equivalent to :write_concern
      #   option.
      # @option options [ Hash ] :write_concern The write concern options.
      #   Can be :w => Integer|String, :fsync => Boolean, :j => Boolean.
      #
      # @return [ BSON::ObjectId ] The ObjectId file id.
      #
      # @since 2.1.0
      def upload_from_stream(filename, io, opts = {})
        open_upload_stream(filename, opts) do |stream|
          begin
            stream.write(io)
          # IOError and SystemCallError are for errors reading the io.
          # Error::SocketError and Error::SocketTimeoutError are for
          # writing to MongoDB.
          rescue IOError, SystemCallError, Error::SocketError, Error::SocketTimeoutError
            begin
              stream.abort
            rescue Error::OperationFailure
            end
            raise
          end
        end.file_id
      end

      # Get the read preference.
      #
      # @note This method always returns a BSON::Document instance, even though
      #   the FSBucket constructor specifies the type of :read as a Hash, not
      #   as a BSON::Document.
      #
      # @return [ BSON::Document ] The read preference.
      #   The document may have the following fields:
      #   - *:mode* -- read preference specified as a symbol; valid values are
      #     *:primary*, *:primary_preferred*, *:secondary*, *:secondary_preferred*
      #     and *:nearest*.
      #   - *:tag_sets* -- an array of hashes.
      #   - *:local_threshold*.
      def read_preference
        @read_preference ||= begin
          pref = options[:read] || database.read_preference
          if BSON::Document === pref
            pref
          else
            BSON::Document.new(pref)
          end
        end
      end

      # Get the write concern.
      #
      # @example Get the write concern.
      #   stream.write_concern
      #
      # @return [ Mongo::WriteConcern ] The write concern.
      #
      # @since 2.1.0
      def write_concern
        @write_concern ||= if wco = @options[:write_concern] || @options[:write]
          WriteConcern.get(wco)
        else
          database.write_concern
        end
      end

      private

      # @param [ Hash ] opts The options.
      #
      # @option opts [ BSON::Document ] :file_info_doc For internal
      #   driver use only. A BSON document to use as file information.
      def read_stream(id, **opts)
        Stream.get(self, Stream::READ_MODE, { file_id: id }.update(options).update(opts))
      end

      def write_stream(filename, **opts)
        Stream.get(self, Stream::WRITE_MODE, { filename: filename }.update(options).update(opts))
      end

      def chunks_name
        "#{prefix}.#{Grid::File::Chunk::COLLECTION}"
      end

      def files_name
        "#{prefix}.#{Grid::File::Info::COLLECTION}"
      end

      def ensure_indexes!
        if files_collection.find({}, limit: 1, projection: { _id: 1 }).first.nil?
          create_index_if_missing!(files_collection, FSBucket::FILES_INDEX)
        end

        if chunks_collection.find({}, limit: 1, projection: { _id: 1 }).first.nil?
          create_index_if_missing!(chunks_collection, FSBucket::CHUNKS_INDEX, :unique => true)
        end
      end

      def create_index_if_missing!(collection, index_spec, options = {})
        indexes_view = collection.indexes
        begin
          if indexes_view.get(index_spec).nil?
            indexes_view.create_one(index_spec, options)
          end
        rescue Mongo::Error::OperationFailure => e
          # proceed with index creation if a NamespaceNotFound error is thrown
          if e.code == 26
            indexes_view.create_one(index_spec, options)
          else
            raise
          end
        end
      end
    end
  end
end
