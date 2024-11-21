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
    class Find

      # A MongoDB find operation sent as an op message.
      #
      # @api private
      #
      # @since 2.5.2
      class OpMsg < OpMsgBase
        include CausalConsistencySupported
        include ExecutableTransactionLabel
        include PolymorphicResult

        private

        # Applies the relevant CSOT timeouts for a find command.
        # Considers the cursor type and timeout mode and will add (or omit) a
        # maxTimeMS field accordingly.
        def apply_relevant_timeouts_to(spec, connection)
          with_max_time(connection) do |max_time_sec|
            timeout_ms = max_time_sec ? (max_time_sec * 1_000).to_i : nil
            apply_find_timeouts_to(spec, timeout_ms) unless connection.description.mongocryptd?
          end
        end

        def apply_find_timeouts_to(spec, timeout_ms)
          view = context&.view
          return spec unless view

          case view.cursor_type
          when nil # non-tailable
            if view.timeout_mode == :cursor_lifetime
              spec[:maxTimeMS] = timeout_ms || view.options[:max_time_ms]
            else # timeout_mode == :iterable
              # drivers MUST honor the timeoutMS option for the initial command
              # but MUST NOT append a maxTimeMS field to the command sent to the
              # server
              if !timeout_ms && view.options[:max_time_ms]
                spec[:maxTimeMS] = view.options[:max_time_ms]
              end
            end

          when :tailable
            # If timeoutMS is set, drivers...MUST NOT append a maxTimeMS field to any commands.
            if !timeout_ms && view.options[:max_time_ms]
              spec[:maxTimeMS] = view.options[:max_time_ms]
            end

          when :tailable_await
            # The server supports the maxTimeMS option for the original command.
            if timeout_ms || view.options[:max_time_ms]
              spec[:maxTimeMS] = timeout_ms || view.options[:max_time_ms]
            end
          end

          spec.tap do |spc|
            spc.delete(:maxTimeMS) if spc[:maxTimeMS].nil?
          end
        end

        def selector(connection)
          # The mappings are BSON::Documents and as such store keys as
          # strings, the spec here has symbol keys.
          spec = BSON::Document.new(self.spec)
          {
            find: coll_name,
            Protocol::Msg::DATABASE_IDENTIFIER => db_name,
          }.update(Find::Builder::Command.selector(spec, connection))
        end
      end
    end
  end
end
