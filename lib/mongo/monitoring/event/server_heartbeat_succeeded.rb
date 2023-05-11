# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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
  class Monitoring
    module Event

      # Event fired when a server heartbeat is dispatched.
      #
      # @since 2.7.0
      class ServerHeartbeatSucceeded < Mongo::Event::Base

        # Create the event.
        #
        # @example Create the event.
        #   ServerHeartbeatSucceeded.new(address, duration)
        #
        # @param [ Address ] address The server address.
        # @param [ Float ] round_trip_time Duration of hello call in seconds.
        # @param [ true | false ] awaited Whether the heartbeat was awaited.
        # @param [ Monitoring::Event::ServerHeartbeatStarted ] started_event
        #   The corresponding started event.
        #
        # @since 2.7.0
        # @api private
        def initialize(address, round_trip_time, awaited: false,
          started_event:
        )
          @address = address
          @round_trip_time = round_trip_time
          @awaited = !!awaited
          @started_event = started_event
        end

        # @return [ Address ] address The server address.
        attr_reader :address

        # @return [ Float ] round_trip_time Duration of hello call in seconds.
        attr_reader :round_trip_time

        # Alias of round_trip_time.
        alias :duration :round_trip_time

        # @return [ true | false ] Whether the heartbeat was awaited.
        def awaited?
          @awaited
        end

        # @return [ Monitoring::Event::ServerHeartbeatStarted ]
        #   The corresponding started event.
        #
        # @api experimental
        attr_reader :started_event

        # Returns a concise yet useful summary of the event.
        #
        # @return [ String ] String summary of the event.
        #
        # @note This method is experimental and subject to change.
        #
        # @since 2.7.0
        # @api experimental
        def summary
          "#<#{short_class_name}" +
          " address=#{address}>"
        end
      end
    end
  end
end
