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
    class Update

      # A MongoDB update operation sent as a command message.
      #
      # @api private
      #
      # @since 2.5.2
      class Command
        include Specifiable
        include Executable
        include Limited
        include WriteConcernSupported
        include BypassDocumentValidation
        include ExecutableNoValidate
        include PolymorphicResult

        private

        def selector(server)
          { update: coll_name,
            updates: send(IDENTIFIER),
            ordered: ordered? }
        end

        def message(server)
          Protocol::Query.new(db_name, Database::COMMAND, command(server), options(server))
        end
      end
    end
  end
end
