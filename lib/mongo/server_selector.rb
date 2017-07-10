# Copyright (C) 2014-2017 MongoDB, Inc.
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

require 'mongo/server_selector/selectable'
require 'mongo/server_selector/nearest'
require 'mongo/server_selector/primary'
require 'mongo/server_selector/primary_preferred'
require 'mongo/server_selector/secondary'
require 'mongo/server_selector/secondary_preferred'

module Mongo

  # Functionality for getting an object able to select a server, given a preference.
  #
  # @since 2.0.0
  module ServerSelector
    extend self

    # The max latency in seconds between the closest server and other servers
    # considered for selection.
    #
    # @since 2.0.0
    LOCAL_THRESHOLD = 0.015.freeze

    # How long to block for server selection before throwing an exception.
    #
    # @since 2.0.0
    SERVER_SELECTION_TIMEOUT = 30.freeze

    # The smallest allowed max staleness value, in seconds.
    #
    # @since 2.4.0
    SMALLEST_MAX_STALENESS_SECONDS = 90

    # Primary read preference.
    #
    # @since 2.1.0
    PRIMARY = Options::Redacted.new(mode: :primary).freeze

    # Hash lookup for the selector classes based off the symbols
    #   provided in configuration.
    #
    # @since 2.0.0
    PREFERENCES = {
      nearest: Nearest,
      primary: Primary,
      primary_preferred: PrimaryPreferred,
      secondary: Secondary,
      secondary_preferred: SecondaryPreferred
    }.freeze

    # Create a server selector object.
    #
    # @example Get a server selector object for selecting a secondary with
    #   specific tag sets.
    #   Mongo::ServerSelector.get(:mode => :secondary, :tag_sets => [{'dc' => 'nyc'}])
    #
    # @param [ Hash ] preference The server preference.
    #
    # @since 2.0.0
    def get(preference = {})
      return preference if PREFERENCES.values.include?(preference.class)
      PREFERENCES.fetch(preference[:mode] || :primary).new(preference)
    end
  end
end
