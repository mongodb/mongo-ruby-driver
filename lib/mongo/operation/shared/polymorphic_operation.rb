# frozen_string_literal: true
# encoding: utf-8

# Copyright (C) 2021 MongoDB Inc.
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

    # Shared behavior of implementing an operation differently based on
    # the server that will be executing the operation.
    #
    # @api private
    module PolymorphicOperation

      # Execute the operation.
      #
      # @param [ Mongo::Server ] server The server to send the operation to.
      # @param [ Operation::Context ] context The operation context.
      # @param [ Hash ] options Operation execution options.
      #
      # @return [ Mongo::Operation::Result ] The operation result.
      def execute(server, context:, options: {})
        server.with_connection do |connection|
          operation = final_operation(connection)
          operation.execute(connection, context: context, options: options)
        end
      end
    end
  end
end
