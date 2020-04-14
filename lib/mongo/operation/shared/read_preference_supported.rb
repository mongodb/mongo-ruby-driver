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

module Mongo
  module Operation

    # Shared behavior of operations that support read preference.
    #
    # @since 2.5.2
    module ReadPreferenceSupported

      private

      SLAVE_OK = :slave_ok

      def options(connection)
        update_options_for_slave_ok(super, connection)
      end

      def update_selector_for_read_pref(sel, connection)
        if read && connection.mongos? && read_pref = read.to_mongos
          Mongo::Lint.validate_camel_case_read_preference(read_pref)
          sel = sel[:$query] ? sel : {:$query => sel}
          sel = sel.merge(:$readPreference => read_pref)
        else
          sel
        end
      end

      def update_options_for_slave_ok(opts, connection)
        if (connection.server.cluster.single? && !connection.mongos?) || (read && read.slave_ok?)
          opts.dup.tap do |o|
            (o[:flags] ||= []) << SLAVE_OK
          end
        else
          opts
        end
      end

      def command(connection)
        sel = super
        update_selector_for_read_pref(sel, connection)
      end
    end
  end
end
