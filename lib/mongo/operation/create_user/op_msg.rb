# Copyright (C) 2018 MongoDB, Inc.
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
    class CreateUser

      # A MongoDB createuser operation sent as an op message.
      #
      # @api private
      #
      # @since 2.5.2
      class OpMsg
        include Specifiable
        include Executable
        include SessionsSupported

        private

        def selector(server)
          { :createUser => user.name }.merge(user.spec)
        end

        def message(server)
          Protocol::Msg.new(flags, options, command(server))
        end
      end
    end
  end
end
