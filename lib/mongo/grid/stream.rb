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

      # A stream that reads and writes files from/to the FSBucket.
      #
      # @since 2.1.0
      class Stream

        # @return [ FSBucket ] fs The fs bucket to which this stream reads/writes.
        attr_reader :fs

        # @return [ String ] filename The filename.
        attr_reader :filename

        # @return [ BSON::ObjectId ] The file id.
        attr_reader :id

        # @return [ BSON::Document, Hash ] The options for the file read/write.
        attr_reader :options

        # Create a stream for reading/writing files from/to the FSBucket.
        #
        # @example Create the stream.
        #   FSBucket::Stream.new('file.txt')
        #
        # @param [ FSBucket ] fs The GridFS bucket object.
        # @param [ String ] filename The name of the file to be streamed.
        # @param [ BSON::Document, Hash ] options The file metadata options.
        #
        # @option options [ String ] :content_type The content type of the file.
        # @option options [ String ] :metadata Optional file metadata.
        # @option options [ Integer ] :chunk_size Override the default chunk
        #   size.
        #
        # @since 2.1.0
        def initialize(fs, filename, options = {})
          @fs = fs
          @filename = filename
          @id = BSON::ObjectId.new
          @options = options
        end

        # Write the data to the FSBucket.
        #
        # @example Write to the FSBucket.
        #   stream.write(file.read)
        #
        # @param [ Object ] data The data to write.
        #
        # @since 2.1.0
        def write(data)
          file = File.new(data, options.merge(:_id => @id, :filename => @filename))
          fs.insert_one(file)
        end
      end
    end
  end
end
