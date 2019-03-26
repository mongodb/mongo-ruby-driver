# Copyright (C) 2014-2019 MongoDB, Inc.
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
  module CRUD

    # Helper module for instantiating either a Read or Write test operation.
    #
    # @since 2.0.0
    module Operation
      extend self

      # Get a new Operation.
      #
      # @example Get the operation.
      #   Operation.get(spec)
      #
      # @param [ Hash ] spec The operation specification.
      #
      # @return [ Operation::Write, Operation::Read ] The Operation object.
      #
      # @since 2.0.0
      def get(spec, outcome_spec = nil)
        if Write::OPERATIONS.keys.include?(spec['name'])
          Write.new(spec, outcome_spec)
        else
          Read.new(spec, outcome_spec)
        end
      end
    end
  end
end
