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
    module Countable

      private

      def count(collection)
        view = collection.find(filter)
        opts = arguments.reduce({}) do |options, (key, value)|
          options.merge!(Mongo::CRUD::Operation::ARGUMENTS[key] => value) unless key == 'filter'
          options
        end
        view.count(opts)
      end
    end
  end
end