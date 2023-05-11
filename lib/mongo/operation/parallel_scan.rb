# frozen_string_literal: true
# rubocop:todo all

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

require 'mongo/operation/parallel_scan/op_msg'
require 'mongo/operation/parallel_scan/result'

module Mongo
  module Operation

    # A MongoDB parallelscan operation.
    #
    # @api private
    #
    # @since 2.0.0
    class ParallelScan
      include Specifiable
      include OpMsgExecutable
    end
  end
end
