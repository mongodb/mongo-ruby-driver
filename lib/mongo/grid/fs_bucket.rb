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
        file_info = files_collection.find(selector).first
        return nil unless file_info
        chunks = chunks_collection.find(:files_id => file_info[:_id]).sort(:n => 1)
        Grid::File.new(chunks.to_a, file_info)
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
        ensure_indexes!
        chunks_collection.insert_many(file.chunks)
        files_collection.insert_one(file.info)
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
      # @since 2.1.0
      def delete(id)
        files_collection.find(:_id => id).delete_one
        chunks_collection.find(:files_id => id).delete_many
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
      # @yieldparam [ Hash ] The read stream.
      #
      # @since 2.1.0
      def open_download_stream(id)
        read_stream(id).tap do |stream|
          if block_given?
            yield stream
            stream.close
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
            io.puts(chunk)
          end
        end
      end

      # Opens an upload stream to GridFS to which the contents of a user file came be written.
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
      # @option opts [ Hash ] :metadata User data for the 'metadata' field of the files
      #   collection document.
      # @option opts [ String ] :content_type The content type of the file.
      #   Deprecated, please use the metadata document instead.
      # @option opts [ Array<String> ] :aliases A list of aliases.
      #   Deprecated, please use the metadata document instead.
      #
      # @return [ Stream::Write ] The write stream.
      #
      # @yieldparam [ Hash ] The write stream.
      #
      # @since 2.1.0
      def open_upload_stream(filename, opts = {})
        write_stream(filename, opts).tap do |stream|
          if block_given?
            yield stream
            stream.close
          end
        end
      end

      # Uploads a user file to a GridFS bucket.
      # Reads the contents of the user file from the source stream and uploads it as chunks in the
      # chunks collection. After all the chunks have been uploaded, it creates a files collection
      # document for the filename in the files collection.
      #
      # @example Open a stream to which the contents of a file came be written.
      #   fs.open_upload_stream('a-file.txt')
      #
      # @param [ String ] filename The filename of the file to upload.
      # @param [ IO ] io The source io stream to upload from.
      # @param [ Hash ] opts The options for the write stream.
      #
      # @option opts [ Integer ] :chunk_size Override the default chunk size.
      # @option opts [ Hash ] :write The write concern.
      # @option opts [ Hash ] :write_concern The write concern.
      # @option opts [ Hash ] :metadata User data for the 'metadata' field of the files
      #   collection document.
      # @option opts [ String ] :content_type The content type of the file. Deprecated, please
      #   use the metadata document instead.
      # @option opts [ Array<String> ] :aliases A list of aliases. Deprecated, please use the
      #   metadata document instead.
      #
      # @return [ BSON::ObjectId ] The ObjectId file id.
      #
      # @since 2.1.0
      def upload_from_stream(filename, io, opts = {})
        open_upload_stream(filename, opts) do |stream|
          stream.write(io)
        end.file_id
      end

      # Get the read preference.
      #
      # @example Get the read preference.
      #   fs.read_preference
      #
      # @return [ Mongo::ServerSelector ] The read preference.
      #
      # @since 2.1.0
      def read_preference
        @read_preference ||= @options[:read] ?
            ServerSelector.get((@options[:read] || {}).merge(database.options)) :
            database.read_preference
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
        @write_concern ||= @options[:write] ? WriteConcern.get(@options[:write]) :
            database.write_concern
      end

      private

      def read_stream(id)
        Stream.get(self, Stream::READ_MODE, { file_id: id }.merge!(options))
      end

      def write_stream(filename, opts)
        Stream.get(self, Stream::WRITE_MODE, { filename: filename }.merge!(options).merge!(opts))
      end

      def chunks_name
        "#{prefix}.#{Grid::File::Chunk::COLLECTION}"
      end

      def files_name
        "#{prefix}.#{Grid::File::Info::COLLECTION}"
      end

      def ensure_indexes!
        if files_collection.find({}, projection: { _id: 1 }).to_a.empty?
          chunks_collection.indexes.create_one(FSBucket::CHUNKS_INDEX, :unique => true)
          files_collection.indexes.create_one(FSBucket::FILES_INDEX)
        end
      end
    end
  end
end
