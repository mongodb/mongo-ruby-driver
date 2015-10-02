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

      # Builds a legacy OP_QUERY specification from options.
      #
      # @since 2.2.0
      class QueryBuilder

        # Options to cursor flags mapping.
        #
        # @since 2.1.0
        CURSOR_FLAGS_MAP = {
          :allow_partial_results => [ :partial ],
          :oplog_replay => [ :oplog_replay ],
          :no_cursor_timeout => [ :no_cursor_timeout ],
          :tailable => [ :tailable_cursor ],
          :tailable_await => [ :await_data, :tailable_cursor]
        }.freeze

        # @return [ Collection ] collection The collection.
        attr_reader :collection

        # @return [ Database ] database The database.
        attr_reader :database

        # @return [ Hash, BSON::Documnet ] filter The filter.
        attr_reader :filter

        # @return [ Hash, BSON::Document ] options The options.
        attr_reader :options

        # Create the new legacy query builder.
        #
        # @example Create the query builder.
        #   QueryBuilder.new(collection, database, {}, {})
        #
        # @param [ Collection ] collection The collection.
        # @param [ Database ] database The database.
        # @param [ Hash, BSON::Document ] filter The filter.
        # @param [ Hash, BSON::Document ] options The options.
        #
        # @since 2.2.2
        def initialize(collection, database, filter, options)
          @collection = collection
          @database = database
          @filter = filter
          @options = options
        end

        def specification
          {
            :selector  => filter,
            :read      => read,
            :options   => options,
            :db_name   => database.name,
            :coll_name => collection.name
          }
        end

        private

        def default_read
          options[:read] || read_preference
        end

        def flags
          @flags ||= CURSOR_FLAGS_MAP.each.reduce([]) do |flags, (key, value)|
            if options[key] || (options[:cursor_type] && options[:cursor_type] == key)
              flags.push(*value)
            end
            flags
          end
        end

        def setup(fil, opts)
          setup_options(opts)
          setup_filter(fil)
        end

        def setup_options(opts)
          @options = opts ? opts.dup : {}
          @modifiers = @options[:modifiers] ? @options.delete(:modifiers).dup : BSON::Document.new
          @options.keys.each { |k| @modifiers.merge!(SPECIAL_FIELDS[k] => @options.delete(k)) if SPECIAL_FIELDS[k] }
          @options.freeze
        end

        def setup_filter(fil)
          @filter = fil ? fil.dup : {}
          if @filter[:$query] || @filter['$query']
            @filter.keys.each { |k| @modifiers.merge!(k => @filter.delete(k)) if k[0] == '$' }
          end
          @modifiers.freeze
          @filter.freeze
        end

        def query_options
          {
            :project => projection,
            :skip => skip,
            :limit => limit,
            :flags => flags,
            :batch_size => batch_size
          }
        end

        def requires_special_filter?
          !modifiers.empty? || cluster.sharded?
        end

        def query_spec
          fil = requires_special_filter? ? special_filter : filter
          { :selector  => fil,
            :read      => read,
            :options   => query_options,
            :db_name   => database.name,
            :coll_name => collection.name }
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
