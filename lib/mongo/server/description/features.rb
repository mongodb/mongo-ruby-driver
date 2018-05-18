# Copyright (C) 2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Server
    class Description

      # Defines behaviour around what features a specific server supports.
      #
      # @since 2.0.0
      class Features

        # List of features and the wire protocol version they appear in.
        #
        # @since 2.0.0
        MAPPINGS = {
          :scram_sha_256 => 7,
          :array_filters => 6,
          :op_msg => 6,
          :sessions => 6,
          :collation => 5,
          :max_staleness => 5,
          :find_command => 4,
          :list_collections => 3,
          :list_indexes => 3,
          :scram_sha_1 => 3,
          :write_command => 2,
          :users_info => 2
        }.freeze

        # Error message if the server is too old for this version of the driver.
        #
        # @since 2.5.0
        SERVER_TOO_OLD = "Server at (%s) reports wire version (%s), but this version of the Ruby driver " +
                           "requires at least (%s)."

        # Error message if the driver is too old for the version of the server.
        #
        # @since 2.5.0
        DRIVER_TOO_OLD = "Server at (%s) requires wire version (%s), but this version of the Ruby driver " +
                           "only supports up to (%s)."

        # The wire protocol versions that this version of the driver supports.
        #
        # @since 2.0.0
        DRIVER_WIRE_VERSIONS = (2..7).freeze

        # Create the methods for each mapping to tell if they are supported.
        #
        # @since 2.0.0
        MAPPINGS.each do |name, version|
          # Determine whether or not the feature is enabled.
          #
          # @example Is a feature enabled?
          #   features.list_collections_enabled?
          #
          # @return [ true, false ] If the feature is enabled.
          #
          # @since 2.0.0
          define_method("#{name}_enabled?") do
            server_wire_versions.include?(MAPPINGS[name])
          end
        end

        # @return [ Range ] server_wire_versions The server's supported wire
        #   versions.
        attr_reader :server_wire_versions

        # Initialize the features.
        #
        # @example Initialize the features.
        #   Features.new(0..3)
        #
        # @param [ Range ] server_wire_versions The server supported wire
        #   versions.
        #
        # @since 2.0.0
        def initialize(server_wire_versions, address = nil)
          @server_wire_versions = server_wire_versions
          @address = address
        end

        # Check that there is an overlap between the driver supported wire version range
        #   and the server wire version range.
        #
        # @example Verify the wire version overlap.
        #   features.check_driver_support!
        #
        # @raise [ Error::UnsupportedFeatures ] If the wire version range is not covered
        #   by the driver.
        #
        # @since 2.5.1
        def check_driver_support!
          if DRIVER_WIRE_VERSIONS.min > @server_wire_versions.max
            raise Error::UnsupportedFeatures.new(SERVER_TOO_OLD % [@address,
                                                                   @server_wire_versions.max,
                                                                   DRIVER_WIRE_VERSIONS.min])
          elsif DRIVER_WIRE_VERSIONS.max < @server_wire_versions.min
            raise Error::UnsupportedFeatures.new(DRIVER_TOO_OLD % [@address,
                                                                   @server_wire_versions.min,
                                                                   DRIVER_WIRE_VERSIONS.max])
          end
        end
      end
    end
  end
end
