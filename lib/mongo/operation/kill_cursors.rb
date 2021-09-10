# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2015-2020 MongoDB Inc.
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

require 'mongo/operation/kill_cursors/command_builder'
require 'mongo/operation/kill_cursors/command'
require 'mongo/operation/kill_cursors/op_msg'
require 'mongo/operation/kill_cursors/legacy'

module Mongo
  module Operation

    # A MongoDB killcursors operation.
    #
    # @api private
    #
    # @since 2.0.0
    class KillCursors
      include Specifiable
      include OpMsgOrFindCommand
    end
  end
end
