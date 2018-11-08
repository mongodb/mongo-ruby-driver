# Copyright (C) 2014-2018 MongoDB, Inc.
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

      # @return [ Integer ] max_staleness The maximum replication lag, in seconds, that a
      #   secondary can suffer and still be eligible for a read.
      #
      # @since 2.4.0
      attr_reader :max_staleness

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
        name == other.name &&
          tag_sets == other.tag_sets &&
            max_staleness == other.max_staleness
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
      # @option options [ Integer ] :local_threshold The local threshold boundary for
      #  nearest selection in seconds.
      # @option options [ Integer ] max_staleness The maximum replication lag,
      #   in seconds, that a secondary can suffer and still be eligible for a read.
      #   A value of -1 is treated identically to nil, which is to not
      #   have a maximum staleness.
      #
      # @raise [ Error::InvalidServerPreference ] If tag sets are specified
      #   but not allowed.
      #
      # @since 2.0.0
      def initialize(options = {})
        @options = (options || {}).freeze
        @tag_sets = (options[:tag_sets] || []).freeze
        @max_staleness = options[:max_staleness] unless options[:max_staleness] == -1
        validate!
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
        "#<#{self.class.name}:0x#{object_id} tag_sets=#{tag_sets.inspect} max_staleness=#{max_staleness.inspect}>"
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
      def select_server(cluster, ping = nil)
        @local_threshold = cluster.options[:local_threshold] || LOCAL_THRESHOLD
        @server_selection_timeout = cluster.options[:server_selection_timeout] || SERVER_SELECTION_TIMEOUT
        deadline = Time.now + server_selection_timeout
        while (deadline - Time.now) > 0
          servers = candidates(cluster)
          if Lint.enabled?
            servers.each do |server|
              if server.average_round_trip_time.nil?
                raise Error::LintError, "Server #{server.address} has nil average rtt"
              end
            end
          end
          if servers && !servers.compact.empty?
            unless cluster.topology.compatible?
              raise Error::UnsupportedFeatures, cluster.topology.compatibility_error.to_s
            end

            # This list of servers may be ordered in a specific way
            # by the selector (e.g. for secondary preferred, the first
            # server may be a secondary and the second server may be primary)
            # and we should take the first server here respecting the order
            server = servers.first

            if cluster.topology.single? &&
              cluster.topology.replica_set_name &&
              cluster.topology.replica_set_name != server.description.replica_set_name
            then
              msg = "Cluster topology specifies replica set name #{cluster.topology.replica_set_name}, but the server has replica set name #{server.description.replica_set_name || '<nil>'}"
              raise Error::NoServerAvailable.new(self, cluster, msg)
            end

            return server
          end
          cluster.scan!(false)
        end
        raise Error::NoServerAvailable.new(self, cluster)
      end

      # Get the timeout for server selection.
      #
      # @example Get the server selection timeout, in seconds.
      #   selector.server_selection_timeout
      #
      # @return [ Float ] The timeout.
      #
      # @since 2.0.0
      #
      # @deprecated This setting is now taken from the cluster options when a server is selected.
      #   Will be removed in 3.0.
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
      #
      # @deprecated This setting is now taken from the cluster options when a server is selected.
      #   Will be removed in 3.0.
      def local_threshold
        @local_threshold ||= (options[:local_threshold] || ServerSelector::LOCAL_THRESHOLD)
      end

      # Get the potential candidates to select from the cluster.
      #
      # @example Get the server candidates.
      #   selectable.candidates(cluster)
      #
      # @param [ Cluster ] cluster The cluster.
      #
      # @return [ Array<Server> ] The candidate servers.
      #
      # @since 2.4.0
      def candidates(cluster)
        if cluster.single?
          cluster.servers.each { |server| validate_max_staleness_support!(server) }
        elsif cluster.sharded?
          near_servers(cluster.servers).each { |server| validate_max_staleness_support!(server) }
        else
          validate_max_staleness_value!(cluster) unless cluster.unknown?
          select(cluster.servers)
        end
      end

      private

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
        matching_servers = filter_stale_servers(matching_servers, primary(candidates).first)
        matching_servers = match_tag_sets(matching_servers) unless tag_sets.empty?
        # Per server selection spec the server selected MUST be a random
        # one matching staleness and latency requirements.
        # Selectors always pass the output of #secondaries to #nearest
        # which shuffles the server list, fulfilling this requirement.
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
        threshold = nearest_server.average_round_trip_time + local_threshold
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

      def filter_stale_servers(candidates, primary = nil)
        return candidates unless @max_staleness

        if primary
          candidates.select do |server|
            validate_max_staleness_support!(server)
            staleness = (server.last_scan - server.last_write_date) -
                        (primary.last_scan - primary.last_write_date)  +
                        server.heartbeat_frequency_seconds
            staleness <= @max_staleness
          end
        else
          max_write_date = candidates.collect(&:last_write_date).max
          candidates.select do |server|
            validate_max_staleness_support!(server)
            staleness = max_write_date - server.last_write_date + server.heartbeat_frequency_seconds
            staleness <= @max_staleness
          end
        end
      end

      def validate!
        if !@tag_sets.all? { |set| set.empty? } && !tags_allowed?
          raise Error::InvalidServerPreference.new(Error::InvalidServerPreference::NO_TAG_SUPPORT)
        elsif @max_staleness && !max_staleness_allowed?
          raise Error::InvalidServerPreference.new(Error::InvalidServerPreference::NO_MAX_STALENESS_SUPPORT)
        end
      end

      def validate_max_staleness_support!(server)
        if @max_staleness && !server.features.max_staleness_enabled?
          raise Error::InvalidServerPreference.new(Error::InvalidServerPreference::NO_MAX_STALENESS_WITH_LEGACY_SERVER)
        end
      end

      def validate_max_staleness_value!(cluster)
        if @max_staleness
          heartbeat_frequency_seconds = cluster.options[:heartbeat_frequency] || Server::Monitor::HEARTBEAT_FREQUENCY
          unless @max_staleness >= [ SMALLEST_MAX_STALENESS_SECONDS,
                                     (heartbeat_frequency_seconds  + Cluster::IDLE_WRITE_PERIOD_SECONDS) ].max
            raise Error::InvalidServerPreference.new(Error::InvalidServerPreference::INVALID_MAX_STALENESS)
          end
        end
      end
    end
  end
end
