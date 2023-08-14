# frozen_string_literal: true

# Copyright (C) 2015-2020 MongoDB Inc.
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

require 'benchmark'
require_relative 'benchmarking/helper'
require_relative 'benchmarking/micro'
require_relative 'benchmarking/single_doc'
require_relative 'benchmarking/multi_doc'
require_relative 'benchmarking/parallel'

module Mongo
  # Module with all functionality for running driver benchmark tests.
  #
  # @since 2.2.3
  module Benchmarking
    extend self

    # @return [ String ] Path to Benchmarking test files.
    DATA_PATH = [ __dir__, 'benchmarking', 'data' ].join('/').freeze

    # @return [ String ] The file containing the single tweet document.
    TWEET_DOCUMENT_FILE = [ DATA_PATH, 'TWEET.json' ].join('/').freeze

    # @return [ String ] The file containing the single small document.
    SMALL_DOCUMENT_FILE = [ DATA_PATH, 'SMALL_DOC.json' ].join('/').freeze

    # @return [ String ] The file containing the single large document.
    LARGE_DOCUMENT_FILE = [ DATA_PATH, 'LARGE_DOC.json' ].join('/').freeze

    # @return [ String ] The file to upload when testing GridFS.
    GRIDFS_FILE = [ DATA_PATH, 'GRIDFS_LARGE' ].join('/').freeze

    # @return [ String ] The file path and base name for the LDJSON files.
    LDJSON_FILE_BASE = [ DATA_PATH, 'LDJSON_MULTI', 'LDJSON' ].join('/').freeze

    # @return [ String ] The file path and base name for the emitted LDJSON files.
    LDJSON_FILE_OUTPUT_BASE = [ DATA_PATH, 'LDJSON_MULTI', 'output', 'LDJSON' ].join('/').freeze

    # @return [ String ] The file path and base name for the GRIDFS files to upload.
    GRIDFS_MULTI_BASE = [ DATA_PATH, 'GRIDFS_MULTI', 'file' ].join('/').freeze

    # @return [ String ] The file path and base name for the emitted GRIDFS downloaded files.
    GRIDFS_MULTI_OUTPUT_BASE = [ DATA_PATH, 'GRIDFS_MULTI', 'output', 'file-output' ].join('/').freeze

    # @return [ Integer ] The number of test repetitions.
    TEST_REPETITIONS = 100

    # Convenience helper for loading the single tweet document.
    #
    # @return [ Hash ] a single parsed JSON document
    def tweet_document
      Benchmarking.load_file(TWEET_DOCUMENT_FILE).first
    end

    # Convenience helper for loading the single small document.
    #
    # @return [ Hash ] a single parsed JSON document
    def small_document
      Benchmarking.load_file(SMALL_DOCUMENT_FILE).first
    end

    # Convenience helper for loading the single large document.
    #
    # @return [ Hash ] a single parsed JSON document
    def large_document
      Benchmarking.load_file(LARGE_DOCUMENT_FILE).first
    end
  end
end
