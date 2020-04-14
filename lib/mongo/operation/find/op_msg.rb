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
    class Find

      # Execute the operation.
      #
      # @example
      #   operation.execute(connection, client: nil)
      #
      # @param [ Mongo::Server::Connection ] connection The connection over which
      #   to send the operation.
      # @param [ Mongo::Client ] client The client that will be used to
      #   perform auto-encryption if it is necessary to encrypt the command
      #   being executed (optional).
      #
      # @return [ Mongo::Operation::Find::Result ] The operation result.
      #
      # @api private
      #
      # @since 2.5.2
      class OpMsg < OpMsgBase
        include CausalConsistencySupported
        include ExecutableTransactionLabel
        include PolymorphicResult
      end
    end
  end
end
