# Copyright (C) 2014-2017 MongoDB, Inc.
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
    module Commands
      class CollectionsInfo

        # Defines custom behaviour of results when query the system.namespaces
        # collection.
        #
        # @since 2.1.0
        class Result < Operation::Result

          # Get the namespace for the cursor.
          #
          # @example Get the namespace.
          #   result.namespace
          #
          # @return [ String ] The namespace.
          #
          # @since 2.1.0
          def namespace
            Database::NAMESPACES
          end
        end
      end
    end
  end
end
