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

require 'mongo/grid/stream/read'
require 'mongo/grid/stream/write'

module Mongo
  module Grid
    class FSBucket

      # A stream that reads and writes files from/to the FSBucket.
      #
      # @since 2.1.0
      module Stream
        extend self

        # The symbol for opening a read stream.
        #
        # @since 2.1.0
        READ_MODE = :r

        # The symbol for opening a write stream.
        #
        # @since 2.1.0
        WRITE_MODE = :w

        # Mapping from mode to stream class.
        #
        # @since 2.1.0
        MODE_MAP = {
            READ_MODE => Read,
            WRITE_MODE => Write
        }.freeze

        # Get a stream for reading/writing files from/to the FSBucket.
        #
        # @example Get a stream.
        #   FSBucket::Stream.get(fs, FSBucket::READ_MODE, options)
        #
        # @param [ FSBucket ] fs The GridFS bucket object.
        # @param [ FSBucket::READ_MODE, FSBucket::WRITE_MODE ] mode The stream mode.
        # @param [ Hash ] options The stream options.
        #
        # @return [ Stream::Read, Stream::Write ] The stream object.
        #
        # @since 2.1.0
        def get(fs, mode, options = {})
          MODE_MAP[mode].new(fs, options)
        end
      end
    end
  end
end
