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
    module Findable

      ARGUMENTS = {
                    'sort' => :sort,
                    'skip' => :skip,
                    'batchSize' => :batch_size,
                    'limit' => :limit
                  }

      private

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