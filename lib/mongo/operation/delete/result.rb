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
  module Operation
    class Delete

      # Defines custom behavior of results for a delete.
      #
      # @since 2.0.0
      # @api semiprivate
      class Result < Operation::Result

        # Get the number of documents deleted.
        #
        # @example Get the deleted count.
        #   result.deleted_count
        #
        # @return [ Integer ] The deleted count.
        #
        # @since 2.0.0
        # @api public
        def deleted_count
          n
        end

        # @api public
        def bulk_result
          BulkResult.new(@replies, connection_description)
        end
      end
    end
  end
end
