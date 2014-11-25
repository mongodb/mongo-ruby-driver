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
  # @since 2.0.0
  module ServerPreference
    extend self

    # Hash lookup for the server preference classes based off the symbols
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

    # Create a server preference object.
    #
    # @example Get a server preference object for selecting a secondary with
    #   specific tag sets and acceptable latency.
    #   Mongo::ServerPreference.get(:mode => :secondary, :tags => [{'tag' => 'set'}])
    #
    # @param [ Hash ] options The read preference options.
    #
    # @option options :mode [ Symbol ] The read preference mode.
    # @option options :tags [ Array<String ] The tag sets.
    #
    # @since 2.0.0
    #
    # @todo: acceptable_latency should be grabbed from a global setting (client)
    def get(options = {})
      PREFERENCES.fetch(options[:mode] || :primary).new(
        options[:tags] || [],
        options[:acceptable_latency] || 15
      )
    end

    # Exception raised if there are no servers available matching server preference.
    #
    # @since 2.0.0
    class NoServerAvailable < DriverError

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::ServerPreference::NoServerAvailable.new(:mode => :secondary)
      #
      # @params [ Hash ] server_preference The server preference that could not be
      #   satisfied.
      #
      # @since 2.0.0
      def initialize(server_preference)
        super("No server is available matching server preference: #{server_preference}")
      end
    end
  end
end
