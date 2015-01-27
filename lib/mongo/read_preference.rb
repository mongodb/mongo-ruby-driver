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

require 'mongo/read_preference/selectable'
require 'mongo/read_preference/nearest'
require 'mongo/read_preference/primary'
require 'mongo/read_preference/primary_preferred'
require 'mongo/read_preference/secondary'
require 'mongo/read_preference/secondary_preferred'

module Mongo

  # Functionality for getting an object representing a specific read preference.
  #
  # @since 2.0.0
  module ReadPreference
    extend self

    # Hash lookup for the read preference classes based off the symbols
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

    # Create a read preference object.
    #
    # @example Get a read preference object for selecting a secondary with
    #   specific tag sets.
    #   Mongo::ReadPreference.get({ :mode => :secondary, :tag_sets => [{'dc' => 'nyc'}] })
    #
    # @param [ Hash ] read The read preference.
    # @param [ Hash ] options The read preference options.
    #
    # @option read :mode [ Symbol ] The read preference mode.
    # @option read :tag_sets [ Array<Hash> ] The tag sets.
    #
    # @since 2.0.0
    def get(read = {}, options = {})
      PREFERENCES.fetch(read[:mode] || :primary).new(
        read[:tag_sets] || [],
        options
      )
    end

    # Exception raised if there are no servers available matching read preference.
    #
    # @since 2.0.0
    class NoServerAvailable < MongoError

      # Instantiate the new exception.
      #
      # @example Instantiate the exception.
      #   Mongo::ReadPreference::NoServerAvailable.new(read_preference)
      #
      # @params [ Hash ] read_preference The read preference that could not be
      #   satisfied.
      #
      # @since 2.0.0
      def initialize(read_preference)
        super("No server is available matching read preference: #{read_preference.inspect}")
      end
    end
  end
end
