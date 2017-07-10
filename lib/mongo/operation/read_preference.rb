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

    # Adds behaviour for updating the options and the selector for operations
    # that need to take read preference into account.
    #
    # @since 2.0.0
    module ReadPreference

      # The constant for slave ok flags.
      #
      # @since 2.0.6
      SLAVE_OK = :slave_ok

      private

      def update_selector_for_read_pref(sel, server)
        if server.mongos? && read_pref = read.to_mongos
          sel = sel[:$query] ? sel : { :$query => sel }
          sel.merge(:$readPreference => read_pref)
        else
          sel
        end
      end

      def slave_ok?(server)
        (server.cluster.single? && !server.mongos?) || read.slave_ok?
      end

      def update_options_for_slave_ok(opts, server)
        if slave_ok?(server)
          opts.dup.tap do |o|
            (o[:flags] ||= []) << SLAVE_OK
          end
        else
          opts
        end
      end

      def message(server)
        sel = update_selector_for_read_pref(selector, server)
        opts = update_options_for_slave_ok(options, server)
        Protocol::Query.new(db_name, query_coll, sel, opts)
      end
    end
  end
end
