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

    # Shared behavior of instantiating a result class matching the
    # operation class.
    #
    # @api private
    module OpMsgOrListIndexesCommand
      include PolymorphicLookup

      def execute(server)
        operation = final_operation(server)
        operation.execute(server)
      end

      private

      def final_operation(server)
        cls = if server.features.op_msg_enabled?
          polymorphic_class(self.class.name, :OpMsg)
        elsif server.features.list_indexes_enabled?
          polymorphic_class(self.class.name, :Command)
        else
          polymorphic_class(self.class.name, :Legacy)
        end
        cls.new(spec)
      end
    end
  end
end
