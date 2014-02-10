# Copyright (C) 2013 10gen Inc.
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
  class Server

    # This object is responsible for keeping server status up to date, running in
    # a separate thread as to not disrupt other operations.
    #
    # @since 3.0.0
    class Refresh

      # @return [ Mongo::Node ] The server the refresher refreshes.
      attr_reader :server
      # @return [ Integer ] The interval the refresh happens on, in seconds.
      attr_reader :interval

      # Create the new server refresher.
      #
      # @example Create the server refresher.
      #   Mongo::Node::Refresh.new(server, 5)
      #
      # @param [ Mongo::Node ] server The server to refresh.
      # @param [ Integer ] interval The refresh interval in seconds.
      #
      # @since 3.0.0
      def initialize(server, interval)
        @server = server
        @interval = interval
      end

      # Runs the server refresher. Refreshing happens on a separate thread per
      # server.
      #
      # @example Run the refresher.
      #   refresher.run
      #
      # @return [ Thread ] The thread the refresher runs on.
      #
      # @since 3.0.0
      def run
        Thread.new(interval, server) do |i, s|
          loop do
            s.refresh!
            sleep(i)
          end
        end
      end
    end
  end
end
