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
    module Commands

      # A MongoDB command operation.
      #
      # @example Create the command operation.
      #   Mongo::Operation::Command.new({ :selector => { :isMaster => 1 } })
      #
      # Initialization:
      #   param [ Hash ] spec The specifications for the command.
      #
      #   option spec :selector [ Hash ] The command selector.
      #   option spec :db_name [ String ] The name of the database on which
      #   the command should be executed.
      #   option spec :options [ Hash ] Options for the command.
      #
      # @since 2.0.0
      class Command
        include Specifiable
        include Limited
        include ReadPreference
        include Executable

        private

        def query_coll
          Database::COMMAND
        end
      end
    end
  end
end
