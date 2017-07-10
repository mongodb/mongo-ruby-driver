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
    module Write

      # This module contains common functionality for operations that need to
      # be followed by a GLE message.
      #
      # @since 2.1.0
      module GLE

        private

        def execute_message(server)
          server.with_connection do |connection|
            result_class = self.class.const_defined?(:LegacyResult, false) ? self.class::LegacyResult :
                self.class.const_defined?(:Result, false) ? self.class::Result : Result
            result_class.new(connection.dispatch([ message(server), gle ].compact)).validate!
          end
        end

        def gle
          wc = write_concern ||  WriteConcern.get(WriteConcern::DEFAULT)
          if gle_message = wc.get_last_error
            Protocol::Query.new(
                db_name,
                Database::COMMAND,
                gle_message,
                options.merge(limit: -1)
            )
          end
        end
      end
    end
  end
end
