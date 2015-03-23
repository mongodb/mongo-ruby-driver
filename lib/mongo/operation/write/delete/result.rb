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
  module Operation
    module Write
      class Delete

        # Defines custom behaviour of results for a delete.
        #
        # @since 2.0.0
        class Result < Operation::Result

          # Get the number of documents deleted.
          #
          # @example Get the deleted count.
          #   result.deleted_count
          #
          # @return [ Integer ] The deleted count.
          #
          # @since 2.0.0
          def deleted_count
            n
          end
        end
      end
    end
  end
end
