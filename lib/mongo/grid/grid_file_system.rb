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

    # A variant on MongoDB GridFS that acts more like a file system, enforcing unique
    # unique filenames and adding versioning for those files.
    #
    # @since 3.0.0
    class GridFileSystem
      include Storable

      # @return [ Integer ] The max number of old versions to store.
      attr_reader :max_versions

      # Create a new GridFileSystem
      #
      # @param [ Mongo::Collection ] files A collection for metadata.
      # @param [ Mongo::Collection ] chunks A collection for the files themselves.
      # @param [ Hash ] opts Additional options for the GridFileStore instance.
      #
      # @options opts [ Integer ] max_versions Max number of old versions of all files
      #   to keep in the system. Once this number is reached, if a new version of the
      #   file is created, the oldest version will be deleted automatically.
      #
      # @since 3.0.0
      def initialize(files, chunks, opts={})
        @files = files
        @chunks = chunks
        @max_versions = opts[:max_versions]
        # @todo db - index these collections
      end

      # Delete old versions of filename.
      #
      # @param [ String ] filename
      # @param [ Integer ] cutoff Number of most recent versions to save.
      #
      # @return [ Integer ] number of versions removed
      #
      # @since 3.0.0
      def delete_old_versions(filename, cutoff=@max_versions)
        # @todo db
        removed = 0
        return 0 unless cutoff && num_versions(filename) > cutoff
        # @files.find({ :filename => filename }).sort({ :uploadDate => -1 }).skip(cutoff).count do |doc|
        to_remove = []
        to_remove.count do |doc|
          delete(doc[:_id])
        end
      end

      private

      # Return the number of versions of this filename that exist in the system.
      #
      # @param [ String ] filename
      #
      # @return [ Integer ] number of versions
      #
      # @since 3.0.0
      def num_versions(filename)
        @files.count({ 'filename' => filename })
      end
    end
  end
end
