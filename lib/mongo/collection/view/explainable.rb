# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2014-2020 MongoDB Inc.
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

      # Defines explain related behavior for collection view.
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

        # Get the query plan for the query.
        #
        # @example Get the query plan for the query with execution statistics.
        #   view.explain(verbosity: :execution_stats)
        #
        # @option opts [ true | false ] :verbose The level of detail
        #   to return for MongoDB 2.6 servers.
        # @option opts [ String | Symbol ] :verbosity The type of information
        #   to return for MongoDB 3.0 and newer servers. If the value is a
        #   symbol, it will be stringified and converted from underscore
        #   style to camel case style (e.g. :query_planner => "queryPlanner").
        #
        # @return [ Hash ] A single document with the query plan.
        #
        # @see https://mongodb.com/docs/manual/reference/method/db.collection.explain/#db.collection.explain
        #
        # @since 2.0.0
        def explain(**opts)
          self.class.new(collection, selector, options.merge(explain_options(**opts))).first
        end

        private

        def explained?
          !!options[:explain]
        end

        # @option opts [ true | false ] :verbose The level of detail
        #   to return for MongoDB 2.6 servers.
        # @option opts [ String | Symbol ] :verbosity The type of information
        #   to return for MongoDB 3.0 and newer servers. If the value is a
        #   symbol, it will be stringified and converted from underscore
        #   style to camel case style (e.g. :query_planner => "queryPlanner").
        def explain_options(**opts)
          explain_limit = limit || 0
          # Note: opts will never be nil here.
          if Symbol === opts[:verbosity]
            opts[:verbosity] = Utils.camelize(opts[:verbosity])
          end
          { limit: -explain_limit.abs, explain: opts }
        end
      end
    end
  end
end
