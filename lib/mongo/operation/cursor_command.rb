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

require 'mongo/operation/cursor_command/op_msg'
require 'mongo/operation/cursor_command/result'

module Mongo
  module Operation
    # A command operation whose response is parsed as a cursor.
    #
    # Unlike Command, the result exposes the firstBatch, namespace, and cursor
    # id from the command response so that a Cursor can be built from it.
    #
    # @api private
    class CursorCommand
      include Specifiable
      include OpMsgExecutable
    end
  end
end
