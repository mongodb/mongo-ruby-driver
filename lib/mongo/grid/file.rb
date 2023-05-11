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

require 'mongo/grid/file/chunk'
require 'mongo/grid/file/info'

module Mongo
  module Grid

    # A representation of a file in the database.
    #
    # @since 2.0.0
    #
    # @deprecated Please use the 'stream' API on a FSBucket instead.
    #   Will be removed in driver version 3.0.
    class File
      extend Forwardable

      # Delegate to file info for convenience.
      def_delegators :info, :chunk_size, :content_type, :filename, :id, :md5, :upload_date

      # @return [ Array<Chunk> ] chunks The file chunks.
      attr_reader :chunks

      # @return [ File::Info ] info The file information.
      attr_reader :info

      # Check equality of files.
      #
      # @example Check the equality of files.
      #   file == other
      #
      # @param [ Object ] other The object to check against.
      #
      # @return [ true, false ] If the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        return false unless other.is_a?(File)
        chunks == other.chunks && info == other.info
      end

      # Initialize the file.
      #
      # @example Create the file.
      #   Grid::File.new(data, :filename => 'test.txt')
      #
      # @param [ IO, String, Array<BSON::Document> ] data The file object, file
      #   contents or chunks.
      # @param [ BSON::Document, Hash ] options The info options.
      #
      # @option options [ String ] :filename Required name of the file.
      # @option options [ String ] :content_type The content type of the file.
      #   Deprecated, please use the metadata document instead.
      # @option options [ String ] :metadata Optional file metadata.
      # @option options [ Integer ] :chunk_size Override the default chunk
      #   size.
      # @option opts [ Array<String> ] :aliases A list of aliases.
      #   Deprecated, please use the metadata document instead.
      #
      # @since 2.0.0
      def initialize(data, options = {})
        options = options.merge(:length => data.size) unless options[:length]
        @info = Info.new(options)
        initialize_chunks!(data)
      end

      # Joins chunks into a string.
      #
      # @return [ String ] The raw data for the file.
      #
      # @since 2.0.0
      def data
        @data ||= Chunk.assemble(chunks)
      end

      # Gets a pretty inspection of the file.
      #
      # @example Get the file inspection.
      #   file.inspect
      #
      # @return [ String ] The file inspection.
      #
      # @since 2.0.0
      def inspect
        "#<Mongo::Grid::File:0x#{object_id} filename=#{filename}>"
      end

      private

      # @note If we have provided an array of BSON::Documents to initialize
      #   with, we have an array of chunk documents and need to create the
      #   chunk objects and assemble the data. If we have an IO object, then
      #   it's the original file data and we must split it into chunks and set
      #   the original data itself.
      #
      # @param [ IO, String, Array<BSON::Document> ] value The file object,
      #   file contents or chunk documents.
      #
      # @return [ Array<Grid::File::Chunk> ] Array of chunks.
      def initialize_chunks!(value)
        if value.is_a?(Array)
          @chunks = value.map{ |doc| Chunk.new(doc) }
        else
          @chunks = Chunk.split(value, info)
        end
      end
    end
  end
end
