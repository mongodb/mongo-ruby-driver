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

      # This module contains common functionality for operations that send either
      # a write command or a specific wire protocol message, depending on server version.
      # For server versions >= 2.6, a write command is sent.
      #
      # @since 2.1.0
      module WriteCommandEnabled

        # Execute the operation.
        #
        # @example Execute the operation.
        #   operation.execute(context)
        #
        # @param [ Mongo::Server::Context ] context The context for this operation.
        #
        # @return [ Result ] The operation result.
        #
        # @since 2.1.0
        def execute(context)
          if context.features.write_command_enabled?
            execute_write_command(context)
          else
            execute_message(context)
          end
        end

        private

        def execute_write_command(context)
          result_class = self.class.const_defined?(:Result, false) ? self.class::Result : Result
          result_class.new(write_command_op.execute(context)).validate!
        end
      end
    end
  end
end
