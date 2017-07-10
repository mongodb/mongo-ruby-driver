
# Copyright (C) 2014-2017 MongoDB, Inc.
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

      # A MongoDB create user operation.
      #
      # @example Initialize the operation.
      #   Write::CreateUser.new(:db_name => 'test', :user => user)
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the create.
      #
      #   option spec :user [ Auth::User ] The user to create.
      #   option spec :db_name [ String ] The name of the database.
      #
      # @since 2.0.0
      class CreateUser
        include GLE
        include WriteCommandEnabled
        include Specifiable

        private

        def write_command_op
          Command::CreateUser.new(spec)
        end

        def message(server)
          user_spec = { user: user.name }.merge(user.spec)
          Protocol::Insert.new(db_name, Auth::User::COLLECTION, [ user_spec ])
        end
      end
    end
  end
end
