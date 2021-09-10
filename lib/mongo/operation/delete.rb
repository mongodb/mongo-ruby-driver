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

require 'mongo/operation/delete/command'
require 'mongo/operation/delete/op_msg'
require 'mongo/operation/delete/legacy'
require 'mongo/operation/delete/result'
require 'mongo/operation/delete/bulk_result'

module Mongo
  module Operation

    # A MongoDB delete operation.
    #
    # @api private
    #
    # @since 2.0.0
    class Delete
      include Specifiable
      include Write

      private

      IDENTIFIER = 'deletes'.freeze
    end
  end
end
