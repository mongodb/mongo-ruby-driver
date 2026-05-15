# frozen_string_literal: true

# Copyright (C) 2026-present MongoDB Inc.
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
  module StreamProcessing
    # The result of a call to {Processor#samples}.
    #
    # Callers MUST stop iterating when {#cursor_id} is 0 — the cursor is
    # exhausted and no further calls should be made.
    #
    # @since 2.25.0
    class SamplesResult
      # @return [ Integer ] The cursor id to pass to the next call.
      #   A value of 0 means the cursor is exhausted.
      attr_reader :cursor_id

      # @return [ Array<Hash> ] The batch of sampled documents.
      attr_reader :documents

      # @param cursor_id [ Integer ]
      # @param documents [ Array<Hash> ]
      def initialize(cursor_id, documents)
        @cursor_id = cursor_id
        @documents = documents || []
      end

      # @return [ Boolean ] Whether the cursor is exhausted (cursor_id == 0).
      def exhausted?
        @cursor_id.zero?
      end
    end
  end
end
