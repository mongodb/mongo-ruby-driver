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
  module CRUD
    module Readable

      ARGUMENTS = {
                    'sort' => :sort,
                    'skip' => :skip,
                    'batchSize' => :batch_size,
                    'limit' => :limit
                  }

      def has_results?
        if name == 'aggregate' && arguments['pipeline'].find {|op| op.keys.include?('$out') }
          return false
        else
          return true
        end
      end

      private

      def count(collection)
        view = collection.find(filter)
        opts = arguments.reduce({}) do |options, (key, value)|
          options.merge!(ARGUMENTS[key] => value) unless key == 'filter'
          options
        end
        view.count(opts)
      end

      def aggregate(collection)
        collection.find.tap do |view|
          view = view.batch_size(arguments['batchSize']) if arguments['batchSize']
        end.aggregate(arguments['pipeline']).to_a
      end

      def distinct(collection)
        view = collection.find(filter)
        view.distinct(arguments['fieldName'])
      end

      def find(collection)
        view = collection.find(filter)
        arguments.each do |key, value|
          view = view.send(ARGUMENTS[key], value) unless key == 'filter'
        end
        view.to_a
      end

      def filter
        arguments['filter']
      end

      def arguments
        @spec['arguments']
      end
    end
  end
end