# Copyright (C) 2015-2017 MongoDB, Inc.
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

        # Provides behaviour for mapping modifiers.
        #
        # @since 2.2.0
        module Modifiers
          extend self

          # Mappings from driver options to legacy server values.
          #
          # @since 2.2.0
          DRIVER_MAPPINGS = BSON::Document.new(
            sort: '$orderby',
            hint: '$hint',
            comment: '$comment',
            snapshot: '$snapshot',
            max_scan: '$maxScan',
            max_value: '$max',
            min_value: '$min',
            max_time_ms: '$maxTimeMS',
            return_key: '$returnKey',
            show_disk_loc: '$showDiskLoc',
            explain: '$explain'
          ).freeze

          # Mappings from server values to driver options.
          #
          # @since 2.2.0
          SERVER_MAPPINGS = BSON::Document.new(DRIVER_MAPPINGS.invert).freeze

          # Transform the provided server modifiers to driver options.
          #
          # @example Transform to driver options.
          #   Modifiers.map_driver_options(modifiers)
          #
          # @param [ Hash ] modifiers The modifiers.
          #
          # @return [ BSON::Document ] The driver options.
          #
          # @since 2.2.0
          def self.map_driver_options(modifiers)
            Options::Mapper.transform_documents(modifiers, SERVER_MAPPINGS)
          end

          # Transform the provided options into a document of only server
          # modifiers.
          #
          # @example Map the server modifiers.
          #   Modifiers.map_server_modifiers(options)
          #
          # @param [ Hash, BSON::Document ] options The options.
          #
          # @return [ BSON::Document ] The modifiers.
          #
          # @since 2.2.0
          def self.map_server_modifiers(options)
            Options::Mapper.transform_documents(options, DRIVER_MAPPINGS)
          end
        end
      end
    end
  end
end
