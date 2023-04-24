# frozen_string_literal: true
# rubocop:todo all

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

    # Custom behavior for operations that support causal consistency.
    #
    # @since 2.5.2
    # @api private
    module CausalConsistencySupported

      private

      # Adds causal consistency document to the selector, if one can be
      # constructed.
      #
      # This method overrides the causal consistency addition logic of
      # SessionsSupported and is meant to be used with operations classified
      # as "read operations accepting a read concern", as these are defined
      # in the causal consistency spec.
      #
      # In order for the override to work correctly the
      # CausalConsistencySupported module must be included after
      # SessionsSupported module in target classes.
      def apply_causal_consistency!(selector, connection)
        apply_causal_consistency_if_possible(selector, connection)
      end
    end
  end
end
