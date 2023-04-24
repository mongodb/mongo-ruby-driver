# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2015-2020 MongoDB Inc.
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
  class BulkWrite

    # Combines groups of bulk write operations in no order.
    #
    # @api private
    #
    # @since 2.1.0
    class UnorderedCombiner
      include Transformable
      include Validatable
      include Combineable

      # Combine the requests in order.
      #
      # @api private
      #
      # @example Combine the requests.
      #   combiner.combine
      #
      # @return [ Array<Hash> ] The combined requests.
      #
      # @since 2.1.0
      def combine
        combine_requests({}).map do |name, ops|
          { name => ops }
        end
      end

      private

      def add(operations, name, document)
        (operations[name] ||= []).push(transform(name, document))
        operations
      end
    end
  end
end
