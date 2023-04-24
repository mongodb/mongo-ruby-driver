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

module Mongo
  module Grid
    class File

      # Encapsulates behavior around GridFS files collection file document.
      #
      # @since 2.0.0
      #
      # @deprecated Please use the 'stream' API on a FSBucket instead.
      #   Will be removed in driver version 3.0.
      class Info

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
          :length => :length,
          :metadata => :metadata,
          :upload_date => :uploadDate,
          :aliases => :aliases
        }.freeze

        # Default content type for stored files.
        #
        # @since 2.0.0
        DEFAULT_CONTENT_TYPE = 'binary/octet-stream'.freeze

        # @return [ BSON::Document ] document The files collection document.
        attr_reader :document

        # Is this file information document equal to another?
        #
        # @example Check file information document equality.
        #   file_info == other
        #
        # @param [ Object ] other The object to check against.
        #
        # @return [ true, false ] If the objects are equal.
        #
        # @since 2.0.0
        def ==(other)
          return false unless other.is_a?(Info)
          document == other.document
        end

        # Get the BSON type for a files information document.
        #
        # @example Get the BSON type.
        #   file_info.bson_type
        #
        # @return [ Integer ] The BSON type.
        #
        # @since 2.0.0
        def bson_type
          BSON::Hash::BSON_TYPE
        end

        # Get the file chunk size.
        #
        # @example Get the chunk size.
        #   file_info.chunk_size
        #
        # @return [ Integer ] The chunksize in bytes.
        #
        # @since 2.0.0
        def chunk_size
          document[:chunkSize]
        end

        # Get the file information content type.
        #
        # @example Get the content type.
        #   file_info.content_type
        #
        # @return [ String ] The content type.
        #
        # @since 2.0.0
        def content_type
          document[:contentType]
        end

        # Get the filename from the file information.
        #
        # @example Get the filename.
        #   file_info.filename
        #
        # @return [ String ] The filename.
        def filename
          document[:filename]
        end

        # Get the file id from the file information.
        #
        # @example Get the file id.
        #   file_info.id
        #
        # @return [ BSON::ObjectId ] The file id.
        #
        # @since 2.0.0
        def id
          document[:_id]
        end

        # Create the new file information document.
        #
        # @example Create the new file information document.
        #   Info.new(:filename => 'test.txt')
        #
        # @param [ BSON::Document ] document The document to create from.
        #
        # @since 2.0.0
        def initialize(document)
          @client_md5 = Digest::MD5.new unless document[:disable_md5] == true
          # document contains a mix of user options and keys added
          # internally by the driver, like session.
          # Remove the keys that driver adds but keep user options.
          document = document.reject do |key, value|
            key.to_s == 'session'
          end
          @document = default_document.merge(Options::Mapper.transform(document, MAPPINGS))
        end

        # Get a readable inspection for the object.
        #
        # @example Inspect the file information.
        #   file_info.inspect
        #
        # @return [ String ] The nice inspection.
        #
        # @since 2.0.0
        def inspect
          "#<Mongo::Grid::File::Info:0x#{object_id} chunk_size=#{chunk_size} " +
            "filename=#{filename} content_type=#{content_type} id=#{id} md5=#{md5}>"
        end

        # Get the length of the document in bytes.
        #
        # @example Get the file length from the file information document.
        #   file_info.length
        #
        # @return [ Integer ] The file length.
        #
        # @since 2.0.0
        def length
          document[:length]
        end
        alias :size :length

        # Get the additional metadata from the file information document.
        #
        # @example Get additional metadata.
        #   file_info.metadata
        #
        # @return [ String ] The additional metadata from file information document.
        #
        # @since 2.0.0
        def metadata
          document[:metadata]
        end

        # Get the md5 hash.
        #
        # @example Get the md5 hash.
        #   file_info.md5
        #
        # @return [ String ] The md5 hash as a string.
        #
        # @since 2.0.0
        #
        # @deprecated as of 2.6.0
        def md5
          document[:md5] || @client_md5
        end

        # Update the md5 hash if there is one.
        #
        # @example Update the md5 hash.
        #   file_info.update_md5(bytes)
        #
        # @note This method is transitional and is provided for backwards compatibility.
        # It will be removed when md5 support is deprecated entirely.
        #
        # @param [ String ] bytes The bytes to use to update the digest.
        #
        # @return [ Digest::MD5 ] The md5 hash object.
        #
        # @since 2.6.0
        #
        # @deprecated as of 2.6.0
        def update_md5(bytes)
          md5.update(bytes) if md5
        end

        # Convert the file information document to BSON for storage.
        #
        # @note If no md5 exists in the file information document (it was loaded
        #   from the server and is not a new file) then we digest the md5 and set it.
        #
        # @example Convert the file information document to BSON.
        #   file_info.to_bson
        #
        # @param [ BSON::ByteBuffer ] buffer The encoded BSON buffer to append to.
        # @param [ true, false ] validating_keys Whether keys should be validated when serializing.
        #   This option is deprecated and will not be used. It will removed in version 3.0.
        #
        # @return [ String ] The raw BSON data.
        #
        # @since 2.0.0
        def to_bson(buffer = BSON::ByteBuffer.new, validating_keys = nil)
          if @client_md5 && !document[:md5]
            document[:md5] = @client_md5.hexdigest
          end
          document.to_bson(buffer)
        end

        # Get the upload date.
        #
        # @example Get the upload date.
        #   file_info.upload_date
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
            # MongoDB stores times with millisecond precision
            :uploadDate => Time.now.utc.round(3),
            :contentType => DEFAULT_CONTENT_TYPE
          )
        end
      end
    end
  end
end
