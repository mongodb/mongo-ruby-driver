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

    class FS

      # Create a new Grid::FS.
      #
      # @param [ Mongo::Collection ] files A collection for metadata.
      # @param [ Mongo::Collection ] chunks A collection for file data.
      #
      # @since 2.0.0
      def initialize(files, chunks)
        @files = files
        @chunks = chunks
        # @todo db - index these collections
      end

      # Opens the file identified by id according to the given mode and returns a new
      # Grid::File object. Supported modes are 'r' and 'w'.
      #
      # @param [ String, BSON::ObjectId ] id An identifier for this file. If a String
      #  filename, will create the file if it does not already exist. If a
      #  BSON::ObjectId, will raise an error if the file does not already exist.
      # @param [ String ] mode Either 'r' or 'w'.
      # @param [ Hash ] opts Options for this file.
      #
      # @options opts [ Integer ] (261120) :chunk_size Custom chunk size, in bytes.
      # @options opts [ Array ] :aliases Array of alias strings for this filename.
      # @options opts [ String ] :content_type A valid MIME type for this document.
      # @options opts [ BSON::ObjectId ] :_id A custom files_id for this file.
      # @options opts [ Hash ] :metadata Any additional metadata for this file.
      #
      # @return [ Grid::File ] file.
      #
      # @since 2.0.0
      def open(id, mode, opts={})
        Grid::File.new(id, mode, @files, @chunks, opts)
      end

      # Delete the file identified by id from the Grid::FS.
      #
      # @param [ String, BSON::ObjectId ] id An identifier for this file, a files_id
      #  or a String filename.  If multiple files with this filename exist within the
      #  FS, deletes all of the associated files.
      #
      # @return [ Integer ] the number of files deleted.
      #
      # @since 2.0.0
      def delete(id)
        if id.is_a?(BSON::ObjectId)
          delete_file(id)
          1
        else
          @files.find({ :filename => id }).count do |file|
            delete_file(file[:_id])
          end
        end
      end

      # Does a file with this identifier (filename or files_id) exist within the FS?
      #
      # @param [ String, BSON::ObjectId ] id A files_id or filename.
      #
      # @return [ true, false ] does this file exist?
      #
      # @since 2.0.0
      def exists?(id)
        if id.is_a?(BSON::ObjectId)
          @files.count({ :_id => id }) > 0
        else
          @files.count({ :filename => id }) > 0
        end
      end
      alias :exist? :exists?

      # Return the number of files currently in the FS.
      #
      # @return [ Integer ] size of FS.
      #
      # @since 2.0.0
      def size
        @files.count
      end

      # Return all matching documents from the FS.
      #
      # @param [ Hash ] query Criteria by which to select documents.
      #
      # @return [ Array ] an array of Grid::File objects.
      #
      # @since 2.0.0
      def find(query={})
        @files.find(query).collect do |doc|
          open(doc[:_id], 'r')
        end
      end

      private

      # Remove this file's chunks and metadata from the file store.
      #
      # @param [ BSON::ObjectId ] id A files_id for this file.
      #
      # @since 2.0.0
      def delete_file(id)
        @chunks.remove({ :files_id => id })
        @files.remove({ :_id => id })
      end
    end
  end
end
