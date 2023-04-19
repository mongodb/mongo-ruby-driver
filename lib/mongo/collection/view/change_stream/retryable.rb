# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2017-2020 MongoDB Inc.
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
  class Collection
    class View
      class ChangeStream < Aggregation

        # Behavior around resuming a change stream.
        #
        # @since 2.5.0
        module Retryable

          private

          def read_with_one_retry
            yield
          rescue Mongo::Error => e
            if e.change_stream_resumable?
              yield
            else
              raise(e)
            end
          end
        end
      end
    end
  end
end
