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

module Mongo
  module Operation
    module Read
      class CollectionNames

        # Defines custom behaviour of results when getting a list of
        # collection names from a query on system.namespaces.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # Get the collection names from documents returned when
          # querying system.namespaces.
          #
          # @example Get the collection names.
          #   result.names
          #
          # @return [ Array<String> ] A list of names.
          #
          # @since 2.0.0
          def names
            documents.reduce([]) do |names, document|
              names.tap do |names|
                collection = document['name']
                unless collection.include?('system.')
                  names << collection[collection.index(".") + 1, collection.length]
                end
              end
            end
          end
        end
      end
    end
  end
end
