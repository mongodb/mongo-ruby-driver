# Copyright (C) 2014-2015 MongoDB, Inc.
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

    # Adds behaviour for queries that need to take read preference into account.
    #
    # @since 2.0.0
    module ReadPreferrable

      private

      def message(context)
        sel = (context.mongos? && read_pref = read.to_mongos) ?
                selector.merge(:$readPreference => read_pref) : selector
        opts = context.standalone? || read.slave_ok? ?
                 options.merge(flags: [:slave_ok]) : options
        Protocol::Query.new(db_name, query_coll, sel, opts)
      end
    end
  end
end
