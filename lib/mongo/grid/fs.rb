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
    class FS
      extend Forwardable

      # The default root prefix.
      #
      # @since 2.0.0
      DEFAULT_ROOT = 'fs'.freeze

      # The specification for the chunks index.
      #
      # @since 2.0.0
      INDEX_SPEC = { :files_id => 1, :n => 1 }.freeze

      # @return [ Collection ] chunks_collection The chunks collection.
      attr_reader :chunks_collection

      # @return [ Database ] database The database.
      attr_reader :database

      # @return [ Collection ] files_collection The files collection.
      attr_reader :files_collection

      # Get write concern from database.
      def_delegators :database, :write_concern

      # Find a file in the GridFS.
      #
      # @example Find a file by it's id.
      #   fs.find_one(_id: id)
      #
      # @example Find a file by it's filename.
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
      # @return [ Result ] The result of the insert.
      #
      # @since 2.0.0
      def insert_one(file)
        files_collection.insert_one(file.metadata)
        inserts = file.chunks.reduce([]) do |ops, chunk|
          ops << { :insert_one => chunk }
        end
        result = chunks_collection.bulk_write(inserts, ordered: true)
        validate_md5!(file) if write_concern.get_last_error
        file.id
      end

      # Create the GridFS.
      #
      # @example Create the GridFS.
      #   Grid::FS.new(database)
      #
      # @param [ Database ] database The database the files reside in.
      # @param [ Hash ] options The GridFS options.
      #
      # @option options [ String ] :fs_name The prefix for the files and chunks
      #   collections.
      #
      # @since 2.0.0
      def initialize(database, options = {})
        @database = database
        @options = options
        @chunks_collection = database[chunks_name]
        @files_collection = database[files_name]
        chunks_collection.indexes.create_one(INDEX_SPEC, :unique => true)
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
        @options[:fs_name] || DEFAULT_ROOT
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

      private

      def chunks_name
        "#{prefix}.#{Grid::File::Chunk::COLLECTION}"
      end

      def files_name
        "#{prefix}.#{Grid::File::Metadata::COLLECTION}"
      end

      def validate_md5!(file)
        md5 = database.command(:filemd5 => file.id, :root => prefix).documents[0][:md5]
        raise Error::InvalidFile.new(file.md5, md5) unless file.md5 == md5
      end
    end
  end
end
