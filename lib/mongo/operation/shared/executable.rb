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

module Mongo
  module Operation

    # Shared executable behavior of operations.
    #
    # @since 2.5.2
    module Executable

      def execute(server)
        result = Result.new(dispatch_message(server))
        process_result(result, server)
        result.validate!
      end

      private

      def dispatch_message(server)
        server.with_connection do |connection|
          connection.dispatch([ message(server) ], operation_id)
        end
      end

      def process_result(result, server)
        server.update_cluster_time(result)
        session.process(result) if session
        result
      end
    end
  end
end

