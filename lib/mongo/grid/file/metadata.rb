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
    class File

      # Encapsulates behaviour around GridFS file metadata.
      #
      # @since 2.0.0
      class Metadata

        # Name of the files collection.
        #
        # @since 2.0.0
        COLLECTION = 'files'.freeze

        # Mappings of user supplied fields to db specification.
        #
        # @since 2.0.0
        MAPPINGS = {
          :chunk_size => :chunkSize,
          :content_type => :contentType,
          :filename => :filename,
          :_id => :_id,
          :md5 => :md5,
          :metadata => :metadata,
          :upload_date => :uploadDate
        }.freeze

        # Default content type for stored files.
        #
        # @since 2.0.0
        DEFAULT_CONTENT_TYPE = 'binary/octet-stream'.freeze

        # @return [ BSON::Document ] document The file metadata document.
        attr_reader :document

        # Is this metadata equal to another?
        #
        # @example Check metadata equality.
        #   metadata == other
        #
        # @param [ Object ] other The object to check against.
        #
        # @return [ true, false ] If the objects are equal.
        #
        # @since 2.0.0
        def ==(other)
          return false unless other.is_a?(Metadata)
          document == other.document
        end

        # Get the BSON type for a metadata document.
        #
        # @example Get the BSON type.
        #   metadata.bson_type
        #
        # @return [ Integer ] The BSON type.
        #
        # @since 2.0.0
        def bson_type
          BSON::Hash::BSON_TYPE
        end

        # Get the metadata chunk size.
        #
        # @example Get the chunk size.
        #   metadata.chunk_size
        #
        # @return [ Integer ] The chunksize in bytes.
        #
        # @since 2.0.0
        def chunk_size
          document[:chunkSize]
        end

        # Get the metadata content type.
        #
        # @example Get the content type.
        #   metadata.content_type
        #
        # @return [ String ] The content type.
        #
        # @since 2.0.0
        def content_type
          document[:contentType]
        end

        # Get the metadata filename.
        #
        # @example Get the filename.
        #   metadata.filename
        #
        # @return [ String ] The filename.
        def filename
          document[:filename]
        end

        # Get the metadata id.
        #
        # @example Get the metadata id.
        #   metadata.id
        #
        # @return [ BSON::ObjectId ] The metadata id.
        #
        # @since 2.0.0
        def id
          document[:_id]
        end

        # Create the new metadata document.
        #
        # @example Create the new metadata document.
        #   Metadata.new(:filename => 'test.txt')
        #
        # @param [ BSON::Document ] document The document to create from.
        #
        # @since 2.0.0
        def initialize(document)
          @document = default_document.merge(Options::Mapper.transform(document, MAPPINGS))
          @client_md5 = Digest::MD5.new
        end

        # Get a readable inspection for the object.
        #
        # @example Inspect the metadata.
        #   metadata.inspect
        #
        # @return [ String ] The nice inspection.
        #
        # @since 2.0.0
        def inspect
          "#<Mongo::Grid::File::Metadata:0x#{object_id} chunk_size=#{chunk_size} " +
            "filename=#{filename} content_type=#{content_type} id=#{id} md5=#{md5}>"
        end

        # Get the length of the document in bytes.
        #
        # @example Get the length
        #   metadata.length
        #
        # @return [ Integer ] The length.
        #
        # @since 2.0.0
        def length
          document[:length]
        end
        alias :size :length

        # Get the additional metadata.
        #
        # @example Get additional metadata.
        #   metadata.metadata
        #
        # @return [ String ] The additional metadata.
        #
        # @since 2.0.0
        def metadata
          document[:metadata]
        end

        # Get the md5 hash.
        #
        # @example Get the md5 hash.
        #   metadata.md5
        #
        # @return [ String ] The md5 hash as a string.
        #
        # @since 2.0.0
        def md5
          document[:md5] || @client_md5
        end

        # Conver the metadata to BSON for storage.
        #
        # @note If no md5 exists in the metadata (it was loaded from the server
        #   and is not a new file) then we digest the md5 and set it.
        #
        # @example Convert the metadata to BSON.
        #   metadata.to_bson
        #
        # @param [ String ] encoded The encoded data to append to.
        #
        # @return [ String ] The raw BSON data.
        #
        # @since 2.0.0
        def to_bson(encoded = ''.force_encoding(BSON::BINARY))
          document[:md5] ||= @client_md5.hexdigest
          document.to_bson(encoded)
        end

        # Get the upload date.
        #
        # @example Get the upload date.
        #   metadata.upload_date
        #
        # @return [ Time ] The upload date.
        #
        # @since 2.0.0
        def upload_date
          document[:uploadDate]
        end

        private

        def default_document
          BSON::Document.new(
            :_id => BSON::ObjectId.new,
            :chunkSize => Chunk::DEFAULT_SIZE,
            :uploadDate => Time.now.utc,
            :contentType => DEFAULT_CONTENT_TYPE
          )
        end
      end
    end
  end
end
