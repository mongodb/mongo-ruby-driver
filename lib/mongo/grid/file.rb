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

require 'mongo/grid/file/chunk'

module Mongo
  module Grid

    # A representation of a file in the database.
    #
    # @since 2.0.0
    class File

      # @return [ String ] data The file data.
      attr_reader :data

      # @return [ String ] filename The name of the file.
      attr_reader :filename

      # @return [ BSON::Document ] metadat The file metadata.
      attr_accessor :metadata

      # Initialize the file.
      #
      # @example Create the file.
      #   Grid::File.new('test.txt')
      #
      # @param [ String ] filename The name of the file.
      #
      # @since 2.0.0
      def initialize(filename)
        @filename = filename
        yield(self) if block_given?
      end

      def chunks=(chunks)
        @chunks = chunks
        assemble!
      end

      def data=(data)
        @data = data
        split!
      end

      private

      def assemble!
        # Put the chunks together and set the data.
      end

      def describe!
        # Generate the file metadata document.
      end

      def split!
        # Split the data and set the chunks.
        describe!
      end
    end
  end
end
