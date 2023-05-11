# frozen_string_literal: true
# rubocop:todo all

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

    # Shared behavior of executing the operation as an OpMsg.
    #
    # @api private
    module OpMsgExecutable
      include PolymorphicLookup

      # Execute the operation.
      #
      # @param [ Mongo::Server ] server The server to send the operation to.
      # @param [ Operation::Context ] context The operation context.
      # @param [ Hash ] options Operation execution options.
      #
      # @return [ Mongo::Operation::Result ] The operation result.
      def execute(server, context:, options: {})
        server.with_connection(connection_global_id: context.connection_global_id) do |connection|
          execute_with_connection(connection, context: context, options: options)
        end
      end

      # Execute the operation.
      #
      # @param [ Mongo::Server::Connection ] connection The connection to send
      #   the operation through.
      # @param [ Operation::Context ] context The operation context.
      # @param [ Hash ] options Operation execution options.
      #
      # @return [ Mongo::Operation::Result ] The operation result.
      def execute_with_connection(connection, context:, options: {})
        final_operation.execute(connection, context: context, options: options)
      end

      private

      def final_operation
        polymorphic_class(self.class.name, :OpMsg).new(spec)
      end
    end
  end
end
