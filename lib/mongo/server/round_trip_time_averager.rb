# Copyright (C) 2018 MongoDB, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module Mongo
  class Server
    # @api private
    class RoundTripTimeAverager

      # The weighting factor (alpha) for calculating the average moving
      # round trip time.
      RTT_WEIGHT_FACTOR = 0.2.freeze
      private_constant :RTT_WEIGHT_FACTOR

      def initialize
        @last_round_trip_time = nil
        @average_round_trip_time = nil
      end

      attr_reader :last_round_trip_time
      attr_reader :average_round_trip_time

      def measure
        start = Time.now
        begin
          rv = yield
        rescue Exception => exc
        end
        @last_round_trip_time = Time.now - start

        update_average_round_trip_time

        [rv, exc, last_round_trip_time, average_round_trip_time]
      end

      private

      # This method is separate for testing purposes.
      def update_average_round_trip_time
        @average_round_trip_time = if average_round_trip_time
          RTT_WEIGHT_FACTOR * last_round_trip_time + (1 - RTT_WEIGHT_FACTOR) * average_round_trip_time
        else
          last_round_trip_time
        end
      end
    end
  end
end
