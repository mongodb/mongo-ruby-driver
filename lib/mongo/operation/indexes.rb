# Copyright (C) 2015-2018 MongoDB, Inc.
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

require 'mongo/operation/indexes/command'
require 'mongo/operation/indexes/op_msg'
require 'mongo/operation/indexes/legacy'
require 'mongo/operation/indexes/result'

module Mongo
  module Operation

    # A MongoDB indexes operation.
    #
    # @api private
    #
    # @since 2.0.0
    class Indexes
      include Specifiable
      include OpMsgOrListIndexesCommand
    end
  end
end
