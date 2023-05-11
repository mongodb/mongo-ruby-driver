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
  module CRUD
    class Outcome
      def initialize(spec)
        if spec.nil?
          raise ArgumentError, 'Outcome specification cannot be nil'
        end
        @result = spec['result']
        @collection = spec['collection']
        @error = spec['error']
      end

      def error?
        !!@error
      end

      def collection_data?
        !!collection_data
      end

      # The expected data in the collection as an outcome after running an
      # operation.
      #
      # @return [ Array<Hash> ] The list of documents expected to be in the collection.
      def collection_data
        @collection && @collection['data']
      end

      def collection_name
        @collection && @collection['name']
      end

      # The expected result of running an operation.
      #
      # @return [ Array<Hash> ] The expected result.
      attr_reader :result
    end
  end
end
