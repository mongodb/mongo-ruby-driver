# Copyright (C) 2015 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  module Monitoring
    module Event

      class Started

        # @return [ BSON::Document ] arguments The command arguments.
        attr_reader :arguments

        # @return [ String ] name The name of the command.
        attr_reader :name

        # @return [ String ] database The name of the database.
        attr_reader :database

        # @return [ String ] server The server address.
        attr_reader :server

        def initialize(name, database, arguments, server)
          @name = name
          @database = database
          @arguments = arguments
          @server = server
        end
      end
    end
  end
end
