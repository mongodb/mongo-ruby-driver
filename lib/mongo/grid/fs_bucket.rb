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

    # Represents a view of the GridFS in the database.
    #
    # @since 2.0.0
    class FSBucket

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

      # The symbol for opening a read stream.
      #
      # @since 2.1.0
      READ_MODE = :r

      # The symbol for opening a write stream.
      #
      # @since 2.1.0
      WRITE_MODE = :w

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

      # Find files collection documents matching a given selector.
      #
      # @example Find files collection documents by a filename.
      #   fs.find(filename: 'file.txt')
      #
      # @param [ Hash ] selector The selector to use in the find.
      # @param [ Hash ] options The options for the find.
      #
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
        files_collection.find(selector, options.merge(read: read_preference))
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
      def find_one(selector = nil)
        metadata = files_collection.find(selector).first
        return nil unless metadata
        chunks = chunks_collection.find(:files_id => metadata[:_id]).sort(:n => 1)
        Grid::File.new(chunks.to_a, metadata)
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
      def insert_one(file)
        chunks_collection.insert_many(file.chunks)
        files_collection.insert_one(file.metadata)
        file.id
      end

      # Create the GridFS.
      #
      # @example Create the GridFS.
      #   Grid::FSBucket.new(database)
      #
      # @param [ Database ] database The database the files reside in.
      # @param [ Hash ] options The GridFS options.
      #
      # @option options [ String ] :fs_name The prefix for the files and chunks
      #   collections.
      # @option options [ String ] :bucket_name The prefix for the files and chunks
      #   collections.
      # @option options [ Integer ] :chunk_size Override the default chunk
      #   size.
      # @option options [ String ] :write The write concern.
      # @option options [ String ] :write_concern The write concern.
      # @option options [ String ] :read The read preference.
      #
      # @since 2.0.0
      def initialize(database, options = {})
        @database = database
        @options = options
        @chunks_collection = database[chunks_name]
        @files_collection = database[files_name]
        chunks_collection.indexes.create_one(CHUNKS_INDEX, :unique => true)
        files_collection.indexes.create_one(FILES_INDEX)
      rescue Error::OperationFailure
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
        @options[:fs_name] || @options[:bucket_name]|| DEFAULT_ROOT
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
        files_collection.find(:_id => file.id).delete_one
        chunks_collection.find(:files_id => file.id).delete_many
      end

      # Opens a stream from which a file can be downloaded, specified by id.
      #
      # @example Open a stream from which a file can be downloaded.
      #   fs.open_download_stream(id)
      #
      # @param [ BSON::ObjectId, Object ] id The id of the file to read.
      #
      # @return [ Stream::Read ] The stream to read from.
      #
      # @since 2.1.0
      def open_download_stream(id)
        read_stream(id)
      end

      # Downloads the contents of the file specified by id and writes them to
      # the destination io object.
      #
      # @example Download the file and write it to the io object.
      #   fs.download_to_stream(id, io)
      #
      # @param [ BSON::ObjectId, Object ] id The id of the file to read.
      # @param [ IO ] The io object to write to.
      #
      # @since 2.1.0
      def download_to_stream(id, io)
        read_stream(id).each do |chunk|
          io.puts(chunk)
        end
      end

      # Opens a stream to which the contents of a file came be written.
      #
      # @example Open a stream to which the contents of a file came be written.
      #   fs.open_upload_stream('a-file.txt')
      #
      # @param [ String ] filename The filename of the file to upload.
      # @param [ Hash ] opts The options for the write stream.
      #
      # @option opts [ Integer ] :chunk_size Override the default chunk size.
      # @option opts [ Hash ] :write The write concern.
      # @option opts [ Hash ] :write_concern The write concern.
      # @option opts [ Hash ] :metadata User data for the 'metadata' field of the files collection document.
      # @option opts [ String ] :content_type The content type of the file.
      # @option opts [ Array<String> ] :aliases A list of aliases.
      #
      # @return [ Stream::Write ] The stream to write to.
      #
      # @since 2.1.0
      def open_upload_stream(filename, opts = {})
        write_stream(filename, opts)
      end

      # Uploads a user file to a GridFS bucket.
      # Read the contents of the user file from the source stream and uploads it as chunks in the
      # chunks collection. After all the chunks have been uploaded, it creates a files collection
      # document for the filename in the files collection.
      #
      # @example Open a stream to which the contents of a file came be written.
      #   fs.open_upload_stream('a-file.txt')
      #
      # @param [ String ] filename The filename of the file to upload.
      # @param [ Hash ] opts The options for the write stream.
      #
      # @option opts [ Integer ] :chunk_size Override the default chunk size.
      # @option opts [ Hash ] :write The write concern.
      # @option opts [ Hash ] :write_concern The write concern.
      # @option opts [ Hash ] :metadata User data for the 'metadata' field of the files collection document.
      # @option opts [ String ] :content_type The content type of the file.
      # @option opts [ Array<String> ] :aliases A list of aliases.
      #
      # @return [ Stream::Write ] The stream to write to.
      #
      # @since 2.1.0
      def upload_from_stream(filename, io, opts = {})
        stream = write_stream(filename, opts)
        stream.write(io).close
        stream.file_id
      end

      private

      def read_stream(id)
        Stream.get(self, READ_MODE, { file_id: id }.merge!(options))
      end

      def write_stream(filename, opts)
        Stream.get(self, WRITE_MODE, { filename: filename }.merge!(opts).merge!(options))
      end

      def chunks_name
        "#{prefix}.#{Grid::File::Chunk::COLLECTION}"
      end

      def files_name
        "#{prefix}.#{Grid::File::Metadata::COLLECTION}"
      end

      def read_preference
        @read_preference ||= @options[:read] ?
            ServerSelector.get((@options[:read] || {}).merge(database.options)) :
              database.read_preference
      end

      def write_concern
        @write_concern ||= @options[:write] ? WriteConcern.get(@options[:write]) :
                             @options[:write_concern] ? WriteConcern.get(@options[:write_concern]) :
                               database.write_concern
      end
    end
  end
end
