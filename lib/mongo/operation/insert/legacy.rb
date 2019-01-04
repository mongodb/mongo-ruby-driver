# Copyright (C) 2015-2019 MongoDB, Inc.
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

        # Execute the operation.
        #
        # @example
        #   operation.execute(server)
        #
        # @param [ Mongo::Server ] server The server to send the operation to.
        #
        # @return [ Mongo::Operation::Insert::Result ] The operation result.
        #
        # @since 2.5.2
        def execute(server)
          result = Result.new(dispatch_message(server), @ids)
          process_result(result, server)
          result.validate!
        end

        private

        def selector
          send(IDENTIFIER).first
        end

        def message(server)
          opts = !!options[:continue_on_error] ? { :flags => [:continue_on_error] } : {}
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
