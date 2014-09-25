
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

module Mongo
  module Grid

    # Represents a view of the GridFS in the database.
    #
    # @since 2.0.0
    class FS

      # @return [ Collection ] chunks_collection The chunks collection.
      attr_reader :chunks_collection

      # @return [ Database ] database The GridFS database.
      attr_reader :database

      # @return [ Collection ] files_collection The files collection.
      attr_reader :files_collection

      # Find a file in the GridFS.
      #
      # @example Find a file by it's id.
      #   fs.find(_id: id)
      #
      # @example Find a file by it's filename.
      #   fs.find(filename: 'test.txt')
      #
      # @param [ Hash ] selector The selector.
      #
      # @return [ Grid::File ] The file.
      #
      # @since 2.0.0
      def find(selector = nil)
        metadata = files_collection.find(selector).first
        chunks = chunks_collection.find(:files_id => file[:_id]).sort(:n => 1)
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
        chunks_collection.insert_many(file.chunks)
      end

      # Create the GridFS.
      #
      # @example Create the GridFS.
      #   Grid::FS.new(database)
      #
      # @param [ Database ] database The database the files reside in.
      #
      # @since 2.0.0
      def initialize(database)
        @database = database
        @chunks_collection = database[Grid::File::Chunk::COLLECTION]
        @files_collection = database[Grid::File::Metadata::COLLECTION]
        # @todo Create index here?
      end
    end
  end
end
