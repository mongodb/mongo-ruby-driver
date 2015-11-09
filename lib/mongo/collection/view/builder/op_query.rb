# Copyright (C) 2015 MongoDB, Inc.
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
      module Builder

        # Builds a legacy OP_QUERY specification from options.
        #
        # @since 2.2.0
        class OpQuery
          extend Forwardable

          def_delegators :@view, :cluster, :collection, :database, :filter, :options, :read

          # @return [ BSON::Document ] modifiers The server modifiers.
          attr_reader :modifiers

          # Create the new legacy query builder.
          #
          # @example Create the query builder.
          #   QueryBuilder.new(view)
          #
          # @param [ Collection::View ] view The collection view.
          #
          # @since 2.2.2
          def initialize(view)
            @view = view
            @modifiers = Modifiers.map_server_modifiers(options)
          end

          def specification
            {
              :selector  => requires_special_filter? ? special_filter : filter,
              :read      => read,
              :options   => query_options,
              :db_name   => database.name,
              :coll_name => collection.name
            }
          end

          private

          def query_options
            BSON::Document.new(
              project: options[:projection],
              skip: options[:skip],
              limit: options[:limit],
              flags: Flags.map_flags(options),
              batch_size: options[:batch_size]
            )
          end

          def requires_special_filter?
            !modifiers.empty? || cluster.sharded?
          end

          def read_pref_formatted
            @read_formatted ||= read.to_mongos
          end

          def special_filter
            sel = BSON::Document.new(:$query => filter).merge!(modifiers)
            sel[:$readPreference] = read_pref_formatted unless read_pref_formatted.nil?
            sel
          end
        end
      end
    end
  end
end
