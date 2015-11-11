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

module Mongo
  module ServerSelector

    # Provides common behavior for filtering a list of servers by server mode or tag set.
    #
    # @since 2.0.0
    module Selectable

      # @return [ Hash ] options The options.
      attr_reader :options

      # @return [ Array ] tag_sets The tag sets used to select servers.
      attr_reader :tag_sets

      # Check equality of two server selector.
      #
      # @example Check server selector equality.
      #   preference == other
      #
      # @param [ Object ] other The other preference.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 2.0.0
      def ==(other)
        name == other.name && tag_sets == other.tag_sets
      end

      # Initialize the server selector.
      #
      # @example Initialize the selector.
      #   Mongo::ServerSelector::Secondary.new(:tag_sets => [{'dc' => 'nyc'}])
      #
      # @example Initialize the preference with no options.
      #   Mongo::ServerSelector::Secondary.new
      #
      # @param [ Hash ] options The server preference options.
      #
      # @option options [ Integer ] :server_selection_timeout The timeout in seconds
      #   for selecting a server.
      #
      # @option options [ Integer ] :local_threshold The local threshold boundary for
      #  nearest selection in seconds.
      #
      # @raise [ Error::InvalidServerPreference ] If tag sets are specified
      #   but not allowed.
      #
      # @since 2.0.0
      def initialize(options = {})
        @options = (options || {}).freeze
        tag_sets = options[:tag_sets] || []
        validate_tag_sets!(tag_sets)
        @tag_sets = tag_sets.freeze
      end

      # Inspect the server selector.
      #
      # @example Inspect the server selector.
      #   selector.inspect
      #
      # @return [ String ] The inspection.
      #
      # @since 2.2.0
      def inspect
        "#<#{self.class.name}:0x#{object_id} tag_sets=#{tag_sets.inspect} " +
        "server_selection_timeout=#{server_selection_timeout} local_threshold=#{local_threshold}>"
      end

      # Select a server from eligible candidates.
      #
      # @example Select a server from the cluster.
      #   selector.select_server(cluster)
      #
      # @param [ Mongo::Cluster ] cluster The cluster from which to select an eligible server.
      #
      # @return [ Mongo::Server ] A server matching the server preference.
      #
      # @since 2.0.0
      def select_server(cluster, ping = true)
        deadline = Time.now + server_selection_timeout
        while (deadline - Time.now) > 0
          servers = candidates(cluster)
          if servers && !servers.compact.empty?
            server = servers.first
            # There is no point pinging a standalone as the subsequent scan is
            # not going to change anything about the cluster.
            if ping && !cluster.single?
              return server if server.connectable?
            else
              return server
            end
          end
          cluster.scan!
        end
        raise Error::NoServerAvailable.new(self)
      end

      # Get the timeout for server selection.
      #
      # @example Get the server selection timeout, in seconds.
      #   selector.server_selection_timeout
      #
      # @return [ Float ] The timeout.
      #
      # @since 2.0.0
      def server_selection_timeout
        @server_selection_timeout ||=
          (options[:server_selection_timeout] || ServerSelector::SERVER_SELECTION_TIMEOUT)
      end

      # Get the local threshold boundary for nearest selection in seconds.
      #
      # @example Get the local threshold.
      #   selector.local_threshold
      #
      # @return [ Float ] The local threshold.
      #
      # @since 2.0.0
      def local_threshold
        @local_threshold ||= (options[:local_threshold] || ServerSelector::LOCAL_THRESHOLD)
      end

      private

      def candidates(cluster)
        if cluster.single?
          cluster.servers
        elsif cluster.sharded?
          near_servers(cluster.servers)
        else
          select(cluster.servers)
        end
      end

      # Select the primary from a list of provided candidates.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   primary from.
      #
      # @return [ Array ] The primary.
      #
      # @since 2.0.0
      def primary(candidates)
        candidates.select do |server|
          server.primary?
        end
      end

      # Select the secondaries from a list of provided candidates.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   secondaries from.
      #
      # @return [ Array ] The secondary servers.
      #
      # @since 2.0.0
      def secondaries(candidates)
        matching_servers = candidates.select(&:secondary?)
        matching_servers = match_tag_sets(matching_servers) unless tag_sets.empty?
        matching_servers
      end

      # Select the near servers from a list of provided candidates, taking the
      #   local threshold into account.
      #
      # @param [ Array ] candidates List of candidate servers to select the
      #   near servers from.
      #
      # @return [ Array ] The near servers.
      #
      # @since 2.0.0
      def near_servers(candidates = [])
        return candidates if candidates.empty?
        nearest_server = candidates.min_by(&:average_round_trip_time)
        threshold = nearest_server.average_round_trip_time + (local_threshold * 1000)
        candidates.select { |server| server.average_round_trip_time <= threshold }.shuffle!
      end

      # Select the servers matching the defined tag sets.
      #
      # @param [ Array ] candidates List of candidate servers from which those
      #   matching the defined tag sets should be selected.
      #
      # @return [ Array ] The servers matching the defined tag sets.
      #
      # @since 2.0.0
      def match_tag_sets(candidates)
        matches = []
        tag_sets.find do |tag_set|
          matches = candidates.select { |server| server.matches_tag_set?(tag_set) }
          !matches.empty?
        end
        matches || []
      end

      def validate_tag_sets!(tag_sets)
        if !tag_sets.all? { |set| set.empty? } && !tags_allowed?
          raise Error::InvalidServerPreference.new(name)
        end
      end
    end
  end
end
