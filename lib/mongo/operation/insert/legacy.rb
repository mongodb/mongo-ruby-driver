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

require 'mongo/operation/update/legacy/result'

module Mongo
  module Operation
    class Insert

      # A MongoDB insert operation sent as a legacy wire protocol message.
      #
      # @api private
      #
      # @since 2.5.2
      class Legacy
        include Specifiable
        include Executable
        include Idable

        private

        def get_result(connection, client, options = {})
          # This is a Mongo::Operation::Insert::Result
          Result.new(*dispatch_message(connection, client), @ids)
        end

        def selector
          send(IDENTIFIER).first
        end

        def message(connection)
          opts = if options(connection)[:continue_on_error]
            { :flags => [:continue_on_error] }
          else
            {}
          end
          Protocol::Insert.new(db_name, coll_name, documents, opts)
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
