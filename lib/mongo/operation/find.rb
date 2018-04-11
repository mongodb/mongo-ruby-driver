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

require 'mongo/operation/find/command'
require 'mongo/operation/find/op_msg'
require 'mongo/operation/find/legacy'
require 'mongo/operation/find/result'

module Mongo
  module Operation

    # A MongoDB find operation.
    #
    # @api private
    #
    # @since 2.0.0
    class Find
      include Specifiable

      # Execute the operation.
      #
      # @example
      #   operation.execute(server)
      #
      # @param [ Mongo::Server ] server The server to send the operation to.
      #
      # @return [ Mongo::Operation::Find::Result ] The operation result.
      #
      # @since 2.0.0
      def execute(server)
        if server.features.op_msg_enabled?
          OpMsg.new(spec).execute(server)
        elsif server.features.find_command_enabled?
          Command.new(spec).execute(server)
        else
          Legacy.new(spec).execute(server)
        end
      end
    end
  end
end
