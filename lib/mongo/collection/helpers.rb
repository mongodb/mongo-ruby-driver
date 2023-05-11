# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2022 MongoDB Inc.
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
  class Collection
    # This module contains helper methods collection class.
    #
    # @api private
    module Helpers
      # Executes drop operation and and ignores NamespaceNotFound error.
      #
      # @param [ Operation::Drop ] operation Drop operation to be executed.
      # @param [ Session ] session Session to be use for execution.
      # @param [ Operation::Context ] context Context to use for execution.
      #
      # @return [ Result ] The result of the execution.
      def do_drop(operation, session, context)
        operation.execute(next_primary(nil, session), context: context)
      rescue Error::OperationFailure => ex
        # NamespaceNotFound
        if ex.code == 26 || ex.code.nil? && ex.message =~ /ns not found/
          false
        else
          raise
        end
      end
    end
  end
end
