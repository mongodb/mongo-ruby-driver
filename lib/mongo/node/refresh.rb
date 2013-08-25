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
  class Node

    # This object is responsible for keeping node status up to date, running in
    # a separate thread as to not disrupt other operations.
    #
    # @since 2.0.0
    class Refresh

      # @return [ Mongo::Node ] The node the refresher refreshes.
      attr_reader :node
      # @return [ Integer ] The interval the refresh happens on, in seconds.
      attr_reader :interval

      # Create the new node refresher.
      #
      # @example Create the node refresher.
      #   Mongo::Node::Refresher.new(node, 5)
      #
      # @param [ Mongo::Node ] node The node to refresh.
      # @param [ Integer ] interval The refresh interval in seconds.
      #
      # @since 2.0.0
      def initialize(node, interval)
        @node = node
        @interval = interval
      end

      # Runs the node refresher. Refreshing happens on a separate thread per
      # node.
      #
      # @example Run the refresher.
      #   refresher.run
      #
      # @return [ Thread ] The thread the refresher runs on.
      #
      # @since 2.0.0
      def run
        Thread.new(interval, node) do |i, n|
          loop do
            n.refresh
            sleep(i)
          end
        end
      end
    end
  end
end
