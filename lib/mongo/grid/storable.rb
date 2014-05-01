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
    module Storable

      # Return a GridReader or GridWriter object for the specified file.
      #
      # @param [ String, BSON::ObjectId ] key A filename or files_id
      # @param [ String ] mode Either 'r' or 'w', see the Ruby IO class for a full
      #   description of these modes.
      # @param [ Hash ] opts Additional options for this file.
      #
      # @options opts [ Integer ] (261120) :chunk_size Custom chunk size in bytes
      #   ('w' only).
      # @options opts [ BSON::ObjectId ] :_id Custom ObjectId for the files_id
      #   ('w' only).
      # @options opts [ String ] :fs_name Custom file system prefix ('w' and 'r').
      # @options opts [ Array ] :aliases An array of alias strings for this file
      #   ('w' only).
      # @options opts [ String ] :content_type A valid MIME type for this file
      #   ('w' only)
      # @options opts [ Hash ] :metadata Any additional metadata to store ('w' only).
      #
      # @return [ Mongo::Grid::GridIO::GridWriter, Mongo::Grid::GridIO::GridReader ]
      #   A GridIO object for this file.
      #
      # @since 3.0.0
      def open(key, mode, opts={})
        delete_old_versions(filename) if versioned?
        if mode == 'r'
          return Grid::GridIO::GridReader.new(@files, @chunks, key, opts)
        elsif mode == 'w'
          return Grid::GridIO::GridWriter.new(@files, @chunks, key, opts)
        else
          raise GridError, "Mode must be either 'r' or 'w'"
        end
      end

      # Store the given data in the Grid under a new file 'filename' with the given
      # options.
      #
      # @param [ String, IO ] data
      # @param [ String ] filename
      # @param [ Hash ] opts Options for this file.
      #
      # @options opts [ Integer ] (261120) :chunk_size Custom chunk size, in bytes.
      # @options opts [ BSON::ObjectId ] :_id Custom ObjectId for the files_id.
      # @options opts [ String ] :fs_name Custom file system prefix
      # @options opts [ Array ] :aliases Array of alias strings for this filename.
      # @options opts [ String ] :content_type A valid MIME type for this document.
      # @options opts [ Hash ] :metadata Any additional metadata to store.
      #
      # @return [ BSON::ObjectId ] the files_id for the new file.
      #
      # @since 3.0.0
      def put(data, filename, opts={})
        delete_old_versions(filename) if versioned?
        f = open(filename, 'w', opts)
        f.write(data)
        f.close
        f.files_id
      end

      # The equivalent of open(filename, 'r').
      #
      # @param [ String, BSON::ObjectId ] filename A filename or files_id.
      # @param [ Hash ] opts Options for this file.
      #
      # @options opts [ String ] :fs_name Custom file system prefix
      # @options opts [ Aliases ] :aliases A list of aliases.
      #
      # @return [ Mongo::GridReader ] this file.
      #
      # @since 3.0.0
      def get(filename, opts={})
        open(filename, 'r', opts)
      end

      # Return the number of files currently stored in the Grid.
      #
      # @return [ Integer ] number of files
      #
      # @since 3.0.0
      def count
        @files.count
      end

      # Return the metadata documents for all matching files in the Grid.
      #
      # @param [ Hash ] query Criteria by which to select documents.
      #
      # @return [ Array ] an array of GridReader objects.
      #
      # @since 3.0.0
      def find(query={})
        @files.find(query).collect do |doc|
          open(doc['_id'], 'r')
        end
      end

      # Delete all matching files from the system.
      #
      # @param [ BSON::ObjectId, String ] key A filename or files_id by which to
      #   identify files. If a filename is used, will delete all files with this
      #   filename, including any versioned copies of the file.
      #
      # @return [ Integer ] number of files removed.
      #
      # @since 3.0.0
      def delete(key)
        # @todo db - replace strings with symbols.
        # @todo aliases, spec?
        if key.is_a?(String)
          @files.find({ 'filename' => key }).count do |doc|
            @files.remove({ '_id' => doc['_id'] })
            @chunks.remove({ 'files_id' => doc['_id'] })
          end
        else
          @files.remove({ '_id' => key })
          @chunks.remove({ 'files_id' => key })
          1
        end
      end

      # Removes all files from the system.
      #
      # @since 3.0.0
      def delete_all
        # @todo db - clean up
        @files.remove({})
        @chunks.remove({})
      end

      # Is this system versioned?
      #
      # @note versioned GridFS implementations must include a delete_old_versions
      #    method that takes a filename as an argument.
      #
      # @return [ Boolean ]
      #
      # @since 3.0.0
      def versioned?
        false
      end
    end
  end
end
