# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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
    class GetMore

      # A MongoDB getMore operation sent as an op message.
      #
      # @api private
      #
      # @since 2.5.2
      class OpMsg < OpMsgBase
        include ExecutableTransactionLabel
        include PolymorphicResult
        include CommandBuilder

        private

        # Applies the relevant CSOT timeouts for a getMore command.
        # Considers the cursor type and timeout mode and will add (or omit) a
        # maxTimeMS field accordingly.
        def apply_relevant_timeouts_to(spec, connection)
          with_max_time(connection) do |max_time_sec|
            timeout_ms = max_time_sec ? (max_time_sec * 1_000).to_i : nil
            apply_get_more_timeouts_to(spec, timeout_ms)
          end
        end

        def apply_get_more_timeouts_to(spec, timeout_ms)
          view = context&.view
          return spec unless view

          if view.cursor_type == :tailable_await
            # If timeoutMS is set, drivers MUST apply it to the original operation.
            # Drivers MUST also apply the original timeoutMS value to each next
            # call on the resulting cursor but MUST NOT use it to derive a
            # maxTimeMS value for getMore commands. Helpers for operations that
            # create tailable awaitData cursors MUST also support the
            # maxAwaitTimeMS option. Drivers MUST error if this option is set,
            # timeoutMS is set to a non-zero value, and maxAwaitTimeMS is greater
            # than or equal to timeoutMS. If this option is set, drivers MUST use
            # it as the maxTimeMS field on getMore commands.
            max_await_time_ms = view.respond_to?(:max_await_time_ms) ? view.max_await_time_ms : nil
            spec[:maxTimeMS] = max_await_time_ms if max_await_time_ms
          end

          spec
        end
      end
    end
  end
end
