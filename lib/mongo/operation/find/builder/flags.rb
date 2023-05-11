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
  module Operation
    class Find
      module Builder

        # Provides behavior for converting Ruby options to wire protocol flags
        # when sending find and related commands (e.g. explain).
        #
        # @api private
        module Flags

          # Options to cursor flags mapping.
          MAPPINGS = {
            :allow_partial_results => [ :partial ],
            :oplog_replay => [ :oplog_replay ],
            :no_cursor_timeout => [ :no_cursor_timeout ],
            :tailable => [ :tailable_cursor ],
            :tailable_await => [ :await_data, :tailable_cursor],
            :await_data => [ :await_data ],
            :exhaust => [ :exhaust ],
          }.freeze

          # Converts Ruby find options to an array of flags.
          #
          # Any keys in the input hash that are not options that map to flags
          # are ignored.
          #
          # @param [ Hash, BSON::Document ] options The options.
          #
          # @return [ Array<Symbol> ] The flags.
          module_function def map_flags(options)
            MAPPINGS.each.reduce(options[:flags] || []) do |flags, (key, value)|
              cursor_type = options[:cursor_type]
              if options[key] || (cursor_type && cursor_type == key)
                flags.push(*value)
              end
              flags
            end
          end
        end
      end
    end
  end
end
