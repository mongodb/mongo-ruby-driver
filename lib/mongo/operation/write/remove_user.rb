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

      # A MongoDB remove user operation.
      #
      # @example Create the remove user operation.
      #   Write::RemoveUser.new(:db_name => 'test', :name => name)
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the remove.
      #
      #   option spec :name [ String ] The user name.
      #   option spec :db_name [ String ] The name of the database.
      #
      # @since 2.0.0
      class RemoveUser
        include GLE
        include WriteCommandEnabled
        include Specifiable

        private

        def write_command_op
          Command::RemoveUser.new(spec)
        end

        def message(server)
          Protocol::Delete.new(db_name, Auth::User::COLLECTION, { user: user_name })
        end
      end
    end
  end
end
