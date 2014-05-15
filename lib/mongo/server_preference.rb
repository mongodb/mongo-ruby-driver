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

require 'mongo/server_preference/selectable'
require 'mongo/server_preference/nearest'
require 'mongo/server_preference/primary'
require 'mongo/server_preference/primary_preferred'
require 'mongo/server_preference/secondary'
require 'mongo/server_preference/secondary_preferred'

module Mongo

  # Functionality for getting an object representing a specific server preference.
  #
  # @since 3.0.0
  module ServerPreference
    extend self

    # Hash lookup for the server preference classes based off the symbols
    #   provided in configuration.
    #
    # @since 3.0.0
    PREFERENCES = {
        nearest: Nearest,
        primary: Primary,
        primary_preferred: PrimaryPreferred,
        secondary: Secondary,
        secondary_preferred: SecondaryPreferred
    }.freeze

    # Create a server preference object.
    #
    # @example Get a server preference object for selecting a secondary with
    #   specific tag sets and acceptable latency.
    #   Mongo::ServerPreference.get(:secondary, [{'tag' => 'set'}], 20)
    #
    #  @param [ Symbol ] mode The name of the server preference mode.
    #  @param [ Array ] tag_sets The tag sets to be used when selecting servers.
    #  @param [ Integer ] acceptable_latency (15) The acceptable latency in milliseconds
    #    to be used when selecting servers.
    #
    # @since 3.0.0
    # @todo: acceptable_latency should be grabbed from a global setting (client)
    def get(mode = :primary, tag_sets = [], acceptable_latency = 15)
      PREFERENCES.fetch(mode.to_sym).new(tag_sets, acceptable_latency)
    end
  end
end

