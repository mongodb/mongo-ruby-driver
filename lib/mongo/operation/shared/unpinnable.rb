# Copyright (C) 2014-2019 MongoDB, Inc.
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
    module Unpinnable

      private

      # Unpins the session if the session is pinned and the yielded to block
      # raises errors that are required to unpin the session.
      #
      # @note This method takes the session as an argument because Unpinnable
      #   is included in BulkWrite which does not store the session in the
      #   receiver (despite Specifiable doing so).
      #
      # @param [ Session | nil ] Session to consider.
      def unpin_maybe(session)
        yield
      rescue Mongo::Error => e
        if session
          session.unpin_maybe(e)
        end
        raise
      end
    end
  end
end
