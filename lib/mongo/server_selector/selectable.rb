# Copyright (C) 2014-2019 MongoDB, Inc.
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
      def initialize(options = nil)
        options = options ? options.dup : {}
        if options[:max_staleness] == -1
          options.delete(:max_staleness)
        end
        @options = options.freeze
        @tag_sets = (options[:tag_sets] || []).freeze
        @max_staleness = options[:max_staleness]
        validate!
      end

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
      # @param [ Mongo::Cluster ] cluster The cluster from which to select
      #   an eligible server.
      # @param [ true, false ] ping Whether to ping the server before selection.
      #   Deprecated and ignored.
      # @param [ Session | nil ] session Optional session to take into account
      #   for mongos pinning.
      #
      # @return [ Mongo::Server ] A server matching the server preference.
      #
      # @since 2.0.0
      def select_server(cluster, ping = nil, session = nil)
        if session && session.pinned_server
          if Mongo::Lint.enabled?
            unless cluster.sharded?
              raise Error::LintError, "Session has a pinned server in a non-sharded topology: #{topology}"
            end
          end

          if !session.in_transaction?
            session.unpin
          end

          if server = session.pinned_server
            # Here we assume that a mongos stays in the topology indefinitely.
            # This will no longer be the case once SRV polling is implemented.

            unless server.mongos?
              raise "Pinned server not a mongos: #{server.summary}"
            end

            return server
          end
        end

        if cluster.replica_set?
          validate_max_staleness_value_early!
        end
        if cluster.addresses.empty?
          if Lint.enabled?
            unless cluster.servers.empty?
              raise Error::LintError, "Cluster has no addresses but has servers: #{cluster.servers.map(&:inspect).join(', ')}"
            end
          end
          msg = "Cluster has no addresses, and therefore will never have a server"
          raise Error::NoServerAvailable.new(self, cluster, msg)
        end
=begin Add this check in version 3.0.0
        unless cluster.connected?
          msg = 'Cluster is disconnected'
          raise Error::NoServerAvailable.new(self, cluster, msg)
        end
=end
        server_selection_timeout = cluster.options[:server_selection_timeout] || SERVER_SELECTION_TIMEOUT
        deadline = Time.now + server_selection_timeout
        while (time_remaining = deadline - Time.now) > 0
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

            if session && session.starting_transaction? && cluster.sharded?
              session.pin(server)
            end

            return server
          end
          cluster.scan!(false)
          if cluster.server_selection_semaphore
            cluster.server_selection_semaphore.wait(time_remaining)
          else
            if Lint.enabled?
              raise Error::LintError, 'Waiting for server selection without having a server selection semaphore'
            end
            sleep 0.25
          end
        end

        msg = "No #{name} server is available in cluster: #{cluster.summary} " +
                "with timeout=#{server_selection_timeout}, " +
                "LT=#{local_threshold_with_cluster(cluster)}"
        dead_monitors = []
        cluster.servers_list.each do |server|
          thread = server.monitor.instance_variable_get('@thread')
          if thread.nil? || !thread.alive?
            dead_monitors << server
          end
        end
        if dead_monitors.any?
          msg += ". The following servers have dead monitor threads: #{dead_monitors.map(&:summary).join(', ')}"
        end
        unless cluster.connected?
          msg += ". The cluster is disconnected (client may have been closed)"
        end
        raise Error::NoServerAvailable.new(self, cluster, msg)
      rescue Error::NoServerAvailable => e
        if session && session.in_transaction? && !session.committing_transaction?
          e.add_label('TransientTransactionError')
        end
        if session && session.committing_transaction?
          e.add_label('UnknownTransactionCommitResult')
        end
        raise e
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

      def local_threshold_with_cluster(cluster)
        options[:local_threshold] || cluster.options[:local_threshold] || LOCAL_THRESHOLD
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
          local_threshold = local_threshold_with_cluster(cluster)
          near_servers(cluster.servers, local_threshold).each do |server|
            validate_max_staleness_support!(server)
          end
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
      # @param [ Integer ] local_threshold Local threshold. This parameter
      #   will be required in driver version 3.0.
      #
      # @return [ Array ] The near servers.
      #
      # @since 2.0.0
      def near_servers(candidates = [], local_threshold = nil)
        return candidates if candidates.empty?
        nearest_server = candidates.min_by(&:average_round_trip_time)
        # Default for legacy signarure
        local_threshold ||= self.local_threshold
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

      def validate_max_staleness_value_early!
        if @max_staleness
          unless @max_staleness >= SMALLEST_MAX_STALENESS_SECONDS
            msg = "`max_staleness` value (#{@max_staleness}) is too small - it must be at least " +
              "`Mongo::ServerSelector::SMALLEST_MAX_STALENESS_SECONDS` (#{ServerSelector::SMALLEST_MAX_STALENESS_SECONDS})"
            raise Error::InvalidServerPreference.new(msg)
          end
        end
      end

      def validate_max_staleness_value!(cluster)
        if @max_staleness
          heartbeat_frequency_seconds = cluster.options[:heartbeat_frequency] || Server::Monitor::HEARTBEAT_FREQUENCY
          unless @max_staleness >= [
            SMALLEST_MAX_STALENESS_SECONDS,
            min_cluster_staleness = heartbeat_frequency_seconds + Cluster::IDLE_WRITE_PERIOD_SECONDS,
          ].max
            msg = "`max_staleness` value (#{@max_staleness}) is too small - it must be at least " +
              "`Mongo::ServerSelector::SMALLEST_MAX_STALENESS_SECONDS` (#{ServerSelector::SMALLEST_MAX_STALENESS_SECONDS}) and (the cluster's heartbeat_frequency " +
              "setting + `Mongo::Cluster::IDLE_WRITE_PERIOD_SECONDS`) (#{min_cluster_staleness})"
            raise Error::InvalidServerPreference.new(msg)
          end
        end
      end
    end
  end
end
