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
    class FSBucket

      module Stream
        # A stream that writes files to the FSBucket.
        #
        # @since 2.1.0
        class Write

          # @return [ FSBucket ] fs The fs bucket to which this stream writes.
          #
          # @since 2.1.0
          attr_reader :fs

          # @return [ BSON::ObjectId ] file_id The id of the file being uploaded.
          #
          # @since 2.1.0
          attr_reader :file_id

          # @return [ String ] filename The name of the file being uploaded.
          #
          # @since 2.1.0
          attr_reader :filename

          # @return [ Hash ] options The write stream options.
          #
          # @since 2.1.0
          attr_reader :options

          # Create a stream for writing files to the FSBucket.
          #
          # @example Create the stream.
          #   Stream::Write.new(fs, options)
          #
          # @param [ FSBucket ] fs The GridFS bucket object.
          # @param [ Hash ] options The write stream options.
          #
          # @option opts [ Integer ] :chunk_size Override the default chunk size.
          # @option opts [ Hash ] :write The write concern.
          # @option opts [ Hash ] :metadata User data for the 'metadata' field of the files collection document.
          # @option opts [ String ] :content_type The content type of the file.
          #   Deprecated, please use the metadata document instead.
          # @option opts [ Array<String> ] :aliases A list of aliases.
          #   Deprecated, please use the metadata document instead.
          #
          # @since 2.1.0
          def initialize(fs, options)
            @fs = fs
            @length = 0
            @n = 0
            @file_id = BSON::ObjectId.new
            @options = options
            @filename = @options[:filename]
            @open = true
          end

          # Write to the GridFS bucket from the source stream.
          #
          # @example Write to GridFS.
          #   stream.write(io)
          #
          # @param [ IO ] io The source io stream to upload from.
          #
          # @return [ Stream::Write ] self The write stream itself.
          #
          # @since 2.1.0
          def write(io)
            ensure_open!
            @indexes ||= ensure_indexes!
            @length += io.size
            chunks = File::Chunk.split(io, file_info, @n)
            @n += chunks.size
            chunks_collection.insert_many(chunks) unless chunks.empty?
            self
          end

          # Close the write stream.
          #
          # @example Close the stream.
          #   stream.close
          #
          # @return [ BSON::ObjectId, Object ] The file id.
          #
          # @raise [ Error::ClosedStream ] If the stream is already closed.
          #
          # @since 2.1.0
          def close
            ensure_open!
            update_length
            files_collection.insert_one(file_info)
            @open = false
            file_id
          end

          # Get the write concern used when uploading.
          #
          # @example Get the write concern.
          #   stream.write_concern
          #
          # @return [ Mongo::WriteConcern ] The write concern.
          #
          # @since 2.1.0
          def write_concern
            @write_concern ||= @options[:write] ? WriteConcern.get(@options[:write]) :
              fs.write_concern
          end

          # Is the stream closed.
          #
          # @example Is the stream closed.
          #   stream.closed?
          #
          # @return [ true, false ] Whether the stream is closed.
          #
          # @since 2.1.0
          def closed?
            !@open
          end

          # Abort the upload by deleting all chunks already inserted.
          #
          # @example Abort the write operation.
          #   stream.abort
          #
          # @return [ true ] True if the operation was aborted and the stream is closed.
          #
          # @since 2.1.0
          def abort
            fs.chunks_collection.find(:files_id => file_id).delete_many
            @open = false || true
          end

          private

          def chunks_collection
            with_write_concern(fs.chunks_collection)
          end

          def files_collection
            with_write_concern(fs.files_collection)
          end

          def with_write_concern(collection)
            if write_concern.nil? || (collection.write_concern &&
                collection.write_concern.options == write_concern.options)
              collection
            else
              collection.with(write: write_concern.options)
            end
          end

          def update_length
            file_info.document[:length] = @length
          end

          def file_info
            doc = { length: @length, _id: file_id, filename: filename }
            @file_info ||= File::Info.new(options.merge(doc))
          end

          def ensure_indexes!
            fs.send(:ensure_indexes!)
          end

          def ensure_open!
            raise Error::ClosedStream.new if closed?
          end
        end
      end
    end
  end
end
