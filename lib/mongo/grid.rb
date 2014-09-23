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

require 'mongo/grid/file'
require 'mongo/grid/fs'

module Mongo

  # Provides behaviour around GridFS related operations.
  #
  # @since 2.0.0
  module Grid

    # Name of the chunks collection.
    #
    # @since 2.0.0
    CHUNKS = 'fs_chunks'.freeze

    # Default size for chunks of data.
    #
    # @since 2.0.0
    DEFAULT_CHUNK_SIZE = (255 * 1024).freeze

    # Default content type for stored files.
    #
    # @since 2.0.0
    DEFAULT_CONTENT_TYPE = 'binary/octet-stream'.freeze

    # Name of the files collection.
    #
    # @since 2.0.0
    FILES = 'fs_files'.freeze
  end
end
