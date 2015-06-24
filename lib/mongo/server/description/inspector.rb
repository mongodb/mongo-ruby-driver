# Copyright (C) 2015 MongoDB, Inc.
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

require 'mongo/server/description/inspector/primary_elected'
require 'mongo/server/description/inspector/description_changed'
require 'mongo/server/description/inspector/standalone_discovered'

module Mongo
  class Server
    class Description

      # Handles inspection of an updated server description to determine if
      # events should be fired.
      #
      # @since 2.0.0
      class Inspector

        # Static list of inspections that are performed on the result of an
        # ismaster command in order to generate the appropriate events for the
        # changes.
        #
        # @since 2.0.0
        INSPECTORS = [
            Inspector::StandaloneDiscovered,
            Inspector::DescriptionChanged,
            Inspector::PrimaryElected
        ].freeze

        # @return [ Array ] inspectors The description inspectors.
        attr_reader :inspectors

        # Create the new inspector.
        #
        # @example Create the new inspector.
        #   Inspector.new(listeners)
        #
        # @param [ Event::Listeners ] listeners The event listeners.
        #
        # @since 2.0.0
        def initialize(listeners)
          @inspectors = INSPECTORS.map do |inspector|
            inspector.new(listeners)
          end
        end

        # Run the server description inspector.
        #
        # @example Run the inspector.
        #   inspector.run(description, { 'ismaster' => true })
        #
        # @param [ Description ] description The old description.
        # @param [ Hash ] ismaster The updated ismaster.
        # @param [ Float ] average_round_trip_time The moving average round trip time (ms).
        #
        # @return [ Description ] The new description.
        #
        # @since 2.0.0
        def run(description, ismaster, average_round_trip_time)
          new_description = Description.new(description.address, ismaster, average_round_trip_time)
          inspectors.each do |inspector|
            inspector.run(description, new_description)
          end
          new_description
        end
      end
    end
  end
end
