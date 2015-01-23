
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
      module Command

        # Create user commands on non-legacy servers.
        #
        # @since 2.0.0
        class CreateUser
          include Specifiable
          include Writable

          private

          # The query selector for this create user command operation.
          #
          # @return [ Hash ] The selector describing this create user operation.
          #
          # @since 2.0.0
          def selector
            { :createUser => user.name, :digestPassword => false }.merge(user.spec)
          end
        end
      end
    end
  end
end
