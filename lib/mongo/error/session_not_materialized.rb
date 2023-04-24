# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2022 MongoDB Inc.
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
  class Error

    # This exception is raised when a session is attempted to be used but
    # it was never materialized.
    class SessionNotMaterialized < InvalidSession
      def initialize
        super("The session was not materialized and cannot be used. Use start_session or with_session in order to start a session that will be materialized.")
      end
    end
  end
end
