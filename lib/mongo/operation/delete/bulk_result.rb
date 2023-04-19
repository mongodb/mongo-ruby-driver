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

      # Defines custom behavior of results for a delete when part of a bulk write.
      #
      # @since 2.0.0
      # @api semiprivate
      class BulkResult < Operation::Result
        include Aggregatable

        # Gets the number of documents deleted.
        #
        # @example Get the deleted count.
        #   result.n_removed
        #
        # @return [ Integer ] The number of documents deleted.
        #
        # @since 2.0.0
        # @api public
        def n_removed
          return 0 unless acknowledged?
          @replies.reduce(0) do |n, reply|
            if reply.documents.first[Result::N]
              n += reply.documents.first[Result::N]
            else
              n
            end
          end
        end
      end
    end
  end
end
