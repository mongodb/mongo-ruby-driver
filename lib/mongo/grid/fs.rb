
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

      # @return [ Collection ] chunks The chunks collection.
      attr_reader :chunks

      # @return [ Database ] database The GridFS database.
      attr_reader :database

      # @return [ Collection ] files The files collection.
      attr_reader :files

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
        file = files.find(selector).first
        chunks = chunks.find(:files_id => file[:_id]).sort(:n => 1)

        # @note We call +to_a+ on chunks to force the cursor to flush all the
        #   documents out into an array for us.
        Grid::File.open(file[:filename], Grid::File::READ) do |f|
          f.chunks = chunks.to_a
          f.document = file
        end
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
        files.insert_one(file.document)
        chunks.insert_many(file.chunks)
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
        @chunks = database[Grid::CHUNKS]
        @files = database[Grid::FILES]
      end
    end
  end
end
