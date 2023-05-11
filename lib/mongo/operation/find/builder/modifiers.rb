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

        # Provides behavior for mapping Ruby options to legacy OP_QUERY
        # find modifiers.
        #
        # This module is used in two ways:
        # 1. When Collection#find is invoked with the legacy OP_QUERY
        #    syntax (:$query argument etc.), this module is used to map
        #    the legacy parameters into the Ruby options that normally
        #    are used by applications.
        # 2. When sending a find operation using the OP_QUERY protocol,
        #    this module is used to map the Ruby find options to the
        #    modifiers in the wire protocol message.
        #
        # @api private
        module Modifiers

          # Mappings from Ruby options to OP_QUERY modifiers.
          DRIVER_MAPPINGS = BSON::Document.new(
            comment: '$comment',
            explain: '$explain',
            hint: '$hint',
            max_scan: '$maxScan',
            max_time_ms: '$maxTimeMS',
            max_value: '$max',
            min_value: '$min',
            return_key: '$returnKey',
            show_disk_loc: '$showDiskLoc',
            snapshot: '$snapshot',
            sort: '$orderby',
          ).freeze

          # Mappings from OP_QUERY modifiers to Ruby options.
          SERVER_MAPPINGS = BSON::Document.new(DRIVER_MAPPINGS.invert).freeze

          # Transform the provided OP_QUERY modifiers to Ruby options.
          #
          # @example Transform to driver options.
          #   Modifiers.map_driver_options(modifiers)
          #
          # @param [ Hash ] modifiers The modifiers.
          #
          # @return [ BSON::Document ] The Ruby options.
          module_function def map_driver_options(modifiers)
            Options::Mapper.transform_documents(modifiers, SERVER_MAPPINGS)
          end

          # Transform the provided Ruby options into a document of OP_QUERY
          # modifiers.
          #
          # Accepts both string and symbol keys.
          #
          # The input mapping may contain additional keys that do not map to
          # OP_QUERY modifiers, in which case the extra keys are ignored.
          #
          # @example Map the server modifiers.
          #   Modifiers.map_server_modifiers(options)
          #
          # @param [ Hash, BSON::Document ] options The options.
          #
          # @return [ BSON::Document ] The modifiers.
          module_function def map_server_modifiers(options)
            Options::Mapper.transform_documents(options, DRIVER_MAPPINGS)
          end
        end
      end
    end
  end
end
