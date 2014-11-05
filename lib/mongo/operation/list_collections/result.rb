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
      class ListCollections

        # Defines custom behaviour of results when using the
        # listCollections command.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # The field of collection name information returned.
          #
          # @since 2.0.0
          COLLECTIONS = 'collections'.freeze

          # Get the list of collection names returned from the
          # listCollections command result.
          #
          # @example Get the collection names.
          #   result.names
          #
          # @return [ Array<String> ] The collection names.
          #
          # @since 2.0.0
          def names
            documents[0][COLLECTIONS].map do |document|
              document['name']
            end
          end
        end
      end
    end
  end
end
