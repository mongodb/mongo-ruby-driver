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

        # Provides behaviour for mapping flags.
        #
        # @since 2.2.0
        module Flags
          extend self

          # Options to cursor flags mapping.
          #
          # @since 2.2.0
          MAPPINGS = {
            :allow_partial_results => [ :partial ],
            :oplog_replay => [ :oplog_replay ],
            :no_cursor_timeout => [ :no_cursor_timeout ],
            :tailable => [ :tailable_cursor ],
            :tailable_await => [ :await_data, :tailable_cursor],
            :await_data => [ :await_data ],
            :exhaust => [ :exhaust ]
          }.freeze

          # Maps an array of flags from the provided options.
          #
          # @example Map the flags.
          #   Flags.map_flags(options)
          #
          # @param [ Hash, BSON::Document ] options The options.
          #
          # @return [ Array<Symbol> ] The flags.
          #
          # @since 2.2.0
          def map_flags(options)
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
