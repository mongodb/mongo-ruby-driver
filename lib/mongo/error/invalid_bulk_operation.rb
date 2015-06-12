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
  class Error

    # Exception raised if an non-existent operation type is used.
    #
    # @since 2.0.0
    class InvalidBulkOperation < Error

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::Error::InvalidBulkOperation.new(name)
      #
      # @param [ String ] type The bulk operation type.
      # @param [ Hash ] operation The bulk operation.
      #
      # @since 2.0.0
      def initialize(type, operation)
        super("Invalid document format for bulk #{type} operation: #{operation}.")
      end
    end
  end
end
