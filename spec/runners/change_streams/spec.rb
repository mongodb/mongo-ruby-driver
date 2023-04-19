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

require 'runners/change_streams/test'

module Mongo
  module ChangeStreams
    class Spec

      # @return [ String ] description The spec description.
      #
      # @since 2.6.0
      attr_reader :description

      # Instantiate the new spec.
      #
      # @param [ String ] test_path The path to the file.
      #
      # @since 2.6.0
      def initialize(test_path)
        @spec = ::Utils.load_spec_yaml_file(test_path)
        @description = File.basename(test_path)
        @spec_tests = @spec['tests']
        @collection_name = @spec['collection_name']
        @collection2_name = @spec['collection2_name']
        @database_name = @spec['database_name']
        @database2_name = @spec['database2_name']
      end

      # Get a list of ChangeStreamsTests for each test definition.
      #
      # @example Get the list of ChangeStreamsTests.
      #   spec.tests
      #
      # @return [ Array<ChangeStreamsTest> ] The list of ChangeStreamsTests.
      #
      # @since 2.0.0
      def tests
        @spec_tests.map do |test|
          ChangeStreamsTest.new(self, test,
            @collection_name, @collection2_name, @database_name, @database2_name)
        end
      end
    end
  end
end
