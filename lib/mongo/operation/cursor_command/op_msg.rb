# frozen_string_literal: true

# Copyright (C) 2025 MongoDB Inc.
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
    class CursorCommand
      # A cursor command operation sent as an op message.
      #
      # @api private
      class OpMsg < OpMsgBase
        include PolymorphicResult

        private

        # The user's command is sent verbatim. The driver MUST NOT inspect or
        # modify it; $db, lsid and other internal fields are attached by the
        # shared command building code.
        def selector(_connection)
          spec[:selector].dup
        end
      end
    end
  end
end
