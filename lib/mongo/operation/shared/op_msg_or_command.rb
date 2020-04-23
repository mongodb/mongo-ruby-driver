# Copyright (C) 2018-2020 MongoDB Inc.
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

    # Shared behavior of executing the operation as an OpMsg when supported
    # or as a Command otherwise.
    #
    # @api private
    module OpMsgOrCommand
      include PolymorphicLookup

      def execute(server, client:, options: {})
        server.with_connection do |connection|
          operation = final_operation(connection)
          operation.execute(connection, client: client, options: options)
        end
      end

      private

      def final_operation(connection)
        cls = if connection.features.op_msg_enabled?
          polymorphic_class(self.class.name, :OpMsg)
        else
          polymorphic_class(self.class.name, :Command)
        end
        cls.new(spec)
      end
    end
  end
end
