# Copyright (C) 2009-2014 MongoDB, Inc.
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

      # Provides behaviour around an aggregation pipeline on a collection view.
      #
      # @since 2.0.0
      class Aggregation
        extend Forwardable
        include Enumerable
        include Immutable
        include Explainable

        # @return [ View ] view The collection view.
        attr_reader :view
        # @return [ Array<Hash> ] pipeline The aggregation pipeline.
        attr_reader :pipeline

        # Delegate necessary operations to the view.
        def_delegators :view, :collection

        # Delegate necessary operations to the collection.
        def_delegators :collection, :database

        # Set to true if disk usage is allowed during the aggregation.
        #
        # @example Set disk usage flag.
        #   aggregation.allow_disk_use(true)
        #
        # @param [ true, false ] value The flag value.
        #
        # @return [ true, false, Aggregation ] The aggregation if a value was
        #   set or the value if used as a getter.
        #
        # @since 2.0.0
        def allow_disk_use(value = nil)
          configure(:allowDiskUse, value)
        end

        # Set to true if a cursor should be used during iteration.
        #
        # @example Set the cursor flag.
        #   aggregation.cursor(true)
        #
        # @param [ true, false ] value The flag value.
        #
        # @return [ true, false, Aggregation ] The aggregation if a value was
        #   set or the value if used as a getter.
        #
        # @since 2.0.0
        def cursor(value = nil)
          configure(:cursor, value)
        end

        # Iterator over the results of the aggregation.
        #
        # @example Iterate over the results.
        #   aggregation.each do |doc|
        #     p doc
        #   end
        #
        # @yieldparam [ BSON::Document ] Each returned document.
        #
        # @return [ Enumerator ] The enumerator.
        #
        # @since 2.0.0
        def each
          enumerator = send_initial_query.documents.first['result'].to_enum
          if block_given?
            enumerator.each{ |document| yield document }
          end
          enumerator
        end

        # Initialize the aggregation for the provided collection view, pipeline
        # and options.
        #
        # @param [ Collection::View ] view The collection view.
        # @param [ Array<Hash> ] pipeline The pipeline of operations.
        # @param [ Hash ] options The aggregation options.
        #
        # @since 2.0.0
        def initialize(view, pipeline, options = {})
          @view = view
          @pipeline = pipeline.dup
          @options = options.dup
        end

        private

        def aggregate_spec
          { :aggregate => collection.name, :pipeline => pipeline }.merge!(options)
        end

        def explain_options
          { :explain => true }
        end

        def new(options)
          Aggregation.new(view, pipeline, options)
        end

        def send_initial_query
          database.command(aggregate_spec)
        end
      end
    end
  end
end
