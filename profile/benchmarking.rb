# Copyright (C) 2015 MongoDB, Inc.
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

module Mongo

  # Module with all functionality for running driver benchmark tests.
  #
  # @since 2.2.2
  module Benchmarking

    extend self

    # The current path.
    #
    # @return [ String ] The current path.
    #
    # @since 2.2.2
    CURRENT_PATH = File.expand_path(File.dirname(__FILE__)).freeze

    # The path to data files used in Benchmarking tests.
    #
    # @return [ String ] Path to Benchmarking test files.
    #
    # @since 2.2.2
    DATA_PATH = "#{CURRENT_PATH}/benchmarking/data/".freeze

    # The file containing the single tweet document.
    #
    # @return [ String ] The file containing the tweet document.
    #
    # @since 2.2.2
    TWEET_DOCUMENT_FILE = "#{DATA_PATH}TWEET.json".freeze

    # The file containing the single small document.
    #
    # @return [ String ] The file containing the small document.
    #
    # @since 2.2.2
    SMALL_DOCUMENT_FILE = "#{DATA_PATH}SMALL_DOC.json".freeze

    # The file containing the single large document.
    #
    # @return [ String ] The file containing the large document.
    #
    # @since 2.2.2
    LARGE_DOCUMENT_FILE = "#{DATA_PATH}LARGE_DOC.json".freeze

    # The file to upload when testing GridFS.
    #
    # @return [ String ] The file containing the GridFS test data.
    #
    # @since 2.2.2
    GRIDFS_FILE = "#{DATA_PATH}GRIDFS_LARGE".freeze

    # The default number of test repetitions.
    #
    # @return [ Integer ] The number of test repetitions.
    #
    # @since 2.2.2
    TEST_REPETITIONS = 100.freeze

    # The number of default warmup repetitions of the test to do before
    # recording times.
    #
    # @return [ Integer ] The default number of warmup repetitions.
    #
    # @since 2.2.2
    WARMUP_REPETITIONS = 10.freeze

    def tweet_document
      Benchmarking.load_file(TWEET_DOCUMENT_FILE).first
    end

    def small_document
      Benchmarking.load_file(SMALL_DOCUMENT_FILE).first
    end

    def large_document
      Benchmarking.load_file(LARGE_DOCUMENT_FILE).first
    end
  end
end
