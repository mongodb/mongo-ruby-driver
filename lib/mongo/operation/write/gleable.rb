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
    module Write

      # This module contains common functionality for sending a GetLastError message.
      #
      # @since 2.1.0
      module GLEable

        private

        def gle
          if gle_message = write_concern.get_last_error
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
