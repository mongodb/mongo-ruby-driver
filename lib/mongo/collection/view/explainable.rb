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
  class Collection
    class View

      # Defines explain related behaviour for collection view.
      #
      # @since 2.0.0
      module Explainable

        # The query planner verbosity constant.
        #
        # @since 2.2.0
        QUERY_PLANNER = 'queryPlanner'.freeze

        # The execution stats verbosity constant.
        #
        # @since 2.2.0
        EXECUTION_STATS = 'executionStats'.freeze

        # The all plans execution verbosity constant.
        #
        # @since 2.2.0
        ALL_PLANS_EXECUTION = 'allPlansExecution'.freeze

        # Get the explain plan for the query.
        #
        # @example Get the explain plan for the query.
        #   view.explain
        #
        # @return [ Hash ] A single document with the explain plan.
        #
        # @since 2.0.0
        def explain
          self.class.new(collection, selector, options.merge(explain_options)).first
        end

        private

        def explained?
          !!options[:explain]
        end

        def explain_options
          explain_limit = limit || 0
          { :limit => -explain_limit.abs, :explain => true }
        end
      end
    end
  end
end
