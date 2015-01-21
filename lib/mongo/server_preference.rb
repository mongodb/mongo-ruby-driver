# Copyright (C) 2014-2015 MongoDB, Inc.
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
    #   specific tag sets and local threshold.
    #   Mongo::ServerPreference.get({ :mode => :secondary, :tag_sets => [{'dc' => 'nyc'}] },
     #                              { :local_threshold_ms => 10 })
    #
    # @param [ Hash ] read The read preference.
    # @param [ Hash ] options The read preference options.
    #
    # @option read :mode [ Symbol ] The read preference mode.
    # @option read :tag_sets [ Array<Hash> ] The tag sets.
    #
    # @option options :local_threshold_ms [ Integer ] The local threshold in ms.
    # @option options :server_selection_timeout_ms [ Integer ] The server selection timeout in ms.
    #
    # @since 2.0.0
    def get(read = {}, options = {})
      PREFERENCES.fetch(read[:mode] || :primary).new(
        read[:tag_sets] || [],
        options[:local_threshold_ms] || 15,
        options[:server_selection_timeout_ms] || 30000
      )
    end

    # Exception raised if there are no servers available matching server preference.
    #
    # @since 2.0.0
    class NoServerAvailable < MongoError

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
