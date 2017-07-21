# Copyright (C) 2014-2017 MongoDB, Inc.
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

    # Adds behaviour for updating the selector for operations
    # that may take a write concern.
    #
    # @since 2.4.0
    module UsesOpMsg

      private

      def op_msg(selector, options)
        selector['$db'] = db_name
        selector['$readPreference'] = read.to_doc
        global_args = { type: 0, document: selector }
        Protocol::Msg.new([:none], options, global_args)
      end
    end
  end
end
