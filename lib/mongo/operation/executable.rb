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

    # This module provides the #execute method that many operations use.
    # It makes sure to instantiate the appropriate Result class for the operation's response.
    #
    # @since 2.0.0
    module Executable

      # Execute the operation.
      # The context gets a connection on which the operation
      # is sent in the block.
      #
      # @param [ Mongo::Server::Context ] context The context for this operation.
      #
      # @return [ Result ] The operation response, if there is one.
      #
      # @since 2.0.0
      def execute(context)
        context.with_connection do |connection|
          result_class = self.class.const_defined?(:Result, false) ? self.class::Result : Result
          result_class.new(connection.dispatch([ message(context) ], operation_id)).validate!
        end
      end
    end
  end
end
