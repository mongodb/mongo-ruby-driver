# frozen_string_literal: true
# rubocop:todo all

# Copyright (C) 2018-2020 MongoDB Inc.
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

class Mongo::Cluster
  # Handles SDAM flow for a server description changed event.
  #
  # Updates server descriptions, topology descriptions and publishes
  # SDAM events.
  #
  # SdamFlow is meant to be instantiated once for every server description
  # changed event that needs to be processed.
  #
  # @api private
  class SdamFlow
    extend Forwardable

    def initialize(cluster, previous_desc, updated_desc, awaited: false)
      @cluster = cluster
      @topology = cluster.topology
      @original_desc = @previous_desc = previous_desc
      @updated_desc = updated_desc
      @servers_to_disconnect = []
      @awaited = !!awaited
    end

    attr_reader :cluster

    def_delegators :cluster, :servers_list, :seeds,
      :publish_sdam_event, :log_warn

    # The topology stored in this attribute can change multiple times throughout
    # a single sdam flow (e.g. unknown -> RS no primary -> RS with primary).
    # Events for topology change get sent at the end of flow processing,
    # such that the above example only publishes an unknown -> RS with primary
    # event to the application.
    #
    # @return Mongo::Cluster::Topology The current topology.
    attr_reader :topology

    attr_reader :previous_desc
    attr_reader :updated_desc
    attr_reader :original_desc

    def awaited?
      @awaited
    end

    def_delegators :topology, :replica_set_name

    # Updates descriptions on all servers whose address matches
    # updated_desc's address.
    def update_server_descriptions
      servers_list.each do |server|
        if server.address == updated_desc.address
          # SDAM flow must be run when topology version in the new description
          # is equal to the current topology version, per the example in
          # https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.rst#what-is-the-purpose-of-topologyversion
          unless updated_desc.topology_version_gte?(server.description)
            return false
          end

          @server_description_changed = server.description != updated_desc

          # Always update server description, so that fields that do not
          # affect description equality comparisons but are part of the
          # description are updated.
          server.update_description(updated_desc)
          server.update_last_scan

          # If there was no content difference between descriptions, we
          # still need to run sdam flow, but if the flow produces no change
          # in topology we will omit sending events.
          return true
        end
      end
      false
    end

    def server_description_changed
      @previous_server_descriptions = servers_list.map do |server|
        [server.address.to_s, server.description]
      end

      unless update_server_descriptions
        # All of the transitions require that server whose updated_desc we are
        # processing is still in the cluster (i.e., was not removed as a result
        # of processing another response, potentially concurrently).
        # If update_server_descriptions returned false we have no servers
        # in the topology for the description we are processing, stop.
        return
      end

      case topology
      when Topology::LoadBalanced
        @updated_desc = ::Mongo::Server::Description::LoadBalancer.new(
          updated_desc.address,
        )
        update_server_descriptions
      when Topology::Single
        if topology.replica_set_name
          if updated_desc.replica_set_name != topology.replica_set_name
            log_warn(
              "Server #{updated_desc.address.to_s} has an incorrect replica set name '#{updated_desc.replica_set_name}'; expected '#{topology.replica_set_name}'"
            )
            @updated_desc = ::Mongo::Server::Description.new(updated_desc.address,
              {}, average_round_trip_time: updated_desc.average_round_trip_time)
            update_server_descriptions
          end
        end
      when Topology::Unknown
        if updated_desc.standalone?
          update_unknown_with_standalone
        elsif updated_desc.mongos?
          @topology = Topology::Sharded.new(topology.options, topology.monitoring, self)
        elsif updated_desc.primary?
          @topology = Topology::ReplicaSetWithPrimary.new(
            topology.options.merge(replica_set_name: updated_desc.replica_set_name),
            topology.monitoring, self)
          update_rs_from_primary
        elsif updated_desc.secondary? || updated_desc.arbiter? || updated_desc.other?
          @topology = Topology::ReplicaSetNoPrimary.new(
            topology.options.merge(replica_set_name: updated_desc.replica_set_name),
            topology.monitoring, self)
          update_rs_without_primary
        end
      when Topology::Sharded
        unless updated_desc.unknown? || updated_desc.mongos?
          log_warn(
            "Removing server #{updated_desc.address.to_s} because it is of the wrong type (#{updated_desc.server_type.to_s.upcase}) - expected SHARDED"
          )
          remove
        end
      when Topology::ReplicaSetWithPrimary
        if updated_desc.standalone? || updated_desc.mongos?
          log_warn(
            "Removing server #{updated_desc.address.to_s} because it is of the wrong type (#{updated_desc.server_type.to_s.upcase}) - expected a replica set member"
          )
          remove
          check_if_has_primary
        elsif updated_desc.primary?
          update_rs_from_primary
        elsif updated_desc.secondary? || updated_desc.arbiter? || updated_desc.other?
          update_rs_with_primary_from_member
        else
          check_if_has_primary
        end
      when Topology::ReplicaSetNoPrimary
        if updated_desc.standalone? || updated_desc.mongos?
          log_warn(
            "Removing server #{updated_desc.address.to_s} because it is of the wrong type (#{updated_desc.server_type.to_s.upcase}) - expected a replica set member"
          )
          remove
        elsif updated_desc.primary?
          # Here we change topology type to RS with primary, however
          # while processing updated_desc we may find that its RS name
          # does not match our existing RS name. For this reason
          # is is imperative to NOT pass updated_desc's RS name to
          # topology constructor here.
          # During processing we may remove the server whose updated_desc
          # we are be processing (e.g. the RS name mismatch case again),
          # in which case topoogy type will go back to RS without primary
          # in the check_if_has_primary step.
          @topology = Topology::ReplicaSetWithPrimary.new(
            # Do not pass updated_desc's RS name here
            topology.options,
            topology.monitoring, self)
          update_rs_from_primary
        elsif updated_desc.secondary? || updated_desc.arbiter? || updated_desc.other?
          update_rs_without_primary
        end
      else
        raise ArgumentError, "Unknown topology #{topology.class}"
      end

      verify_invariants
      commit_changes
      disconnect_servers
    end

    # Transitions from unknown to single topology type, when a standalone
    # server is discovered.
    def update_unknown_with_standalone
      if seeds.length == 1
        @topology = Topology::Single.new(
          topology.options, topology.monitoring, self)
      else
        log_warn(
          "Removing server #{updated_desc.address.to_s} because it is a standalone and we have multiple seeds (#{seeds.length})"
        )
        remove
      end
    end

    # Updates topology which must be a ReplicaSetWithPrimary with information
    # from the primary's server description.
    #
    # This method does not change topology type to ReplicaSetWithPrimary -
    # this needs to have been done prior to calling this method.
    #
    # If the primary whose description is being processed is determined to be
    # stale, this method will change the server description and topology
    # type to unknown.
    def update_rs_from_primary
      if topology.replica_set_name.nil?
        @topology = Topology::ReplicaSetWithPrimary.new(
          topology.options.merge(replica_set_name: updated_desc.replica_set_name),
          topology.monitoring, self)
      end

      if topology.replica_set_name != updated_desc.replica_set_name
        log_warn(
          "Removing server #{updated_desc.address.to_s} because it has an " +
          "incorrect replica set name '#{updated_desc.replica_set_name}'; " +
          "expected '#{topology.replica_set_name}'"
        )
        remove
        check_if_has_primary
        return
      end

      if stale_primary?
        @updated_desc = ::Mongo::Server::Description.new(updated_desc.address,
          {}, average_round_trip_time: updated_desc.average_round_trip_time)
        update_server_descriptions
        check_if_has_primary
        return
      end

      if updated_desc.max_wire_version >= 17
        @topology = Topology::ReplicaSetWithPrimary.new(
          topology.options.merge(
            max_election_id: updated_desc.election_id,
            max_set_version: updated_desc.set_version
          ), topology.monitoring, self)
      else
        max_election_id = topology.new_max_election_id(updated_desc)
        max_set_version = topology.new_max_set_version(updated_desc)

        if max_election_id != topology.max_election_id ||
          max_set_version != topology.max_set_version
        then
          @topology = Topology::ReplicaSetWithPrimary.new(
            topology.options.merge(
              max_election_id: max_election_id,
              max_set_version: max_set_version
            ), topology.monitoring, self)
        end
      end

      # At this point we have accepted the updated server description
      # and the topology (both are primary). Commit these changes so that
      # their respective SDAM events are published before SDAM events for
      # server additions/removals that follow
      publish_description_change_event

      servers_list.each do |server|
        if server.address != updated_desc.address
          if server.primary?
            server.update_description(::Mongo::Server::Description.new(
              server.address, {},
              average_round_trip_time: server.description.average_round_trip_time))
          end
        end
      end

      servers = add_servers_from_desc(updated_desc)
      remove_servers_not_in_desc(updated_desc)

      check_if_has_primary

      servers.each do |server|
        server.start_monitoring
      end
    end

    # Updates a ReplicaSetWithPrimary topology from a non-primary member.
    def update_rs_with_primary_from_member
      if topology.replica_set_name != updated_desc.replica_set_name
        log_warn(
          "Removing server #{updated_desc.address.to_s} because it has an " +
          "incorrect replica set name (#{updated_desc.replica_set_name}); " +
          "current set name is #{topology.replica_set_name}"
        )
        remove
        check_if_has_primary
        return
      end

      if updated_desc.me_mismatch?
        log_warn(
          "Removing server #{updated_desc.address.to_s} because it " +
          "reported itself as #{updated_desc.me}"
        )
        remove
        check_if_has_primary
        return
      end

      have_primary = false
      servers_list.each do |server|
        if server.primary?
          have_primary = true
          break
        end
      end

      unless have_primary
        @topology = Topology::ReplicaSetNoPrimary.new(
          topology.options, topology.monitoring, self)
      end
    end

    # Updates a ReplicaSetNoPrimary topology from a non-primary member.
    def update_rs_without_primary
      if topology.replica_set_name.nil?
        @topology = Topology::ReplicaSetNoPrimary.new(
          topology.options.merge(replica_set_name: updated_desc.replica_set_name),
          topology.monitoring, self)
      end

      if topology.replica_set_name != updated_desc.replica_set_name
        log_warn(
          "Removing server #{updated_desc.address.to_s} because it has an " +
          "incorrect replica set name (#{updated_desc.replica_set_name}); " +
          "current set name is #{topology.replica_set_name}"
        )
        remove
        return
      end

      publish_description_change_event

      servers = add_servers_from_desc(updated_desc)

      commit_changes

      servers.each do |server|
        server.start_monitoring
      end

      if updated_desc.me_mismatch?
        log_warn(
          "Removing server #{updated_desc.address.to_s} because it " +
          "reported itself as #{updated_desc.me}"
        )
        remove
        return
      end
    end

    # Adds all servers referenced in the given description (which is
    # supposed to have come from a good primary) which are not
    # already in the cluster, to the cluster.
    #
    # @note Servers are added unmonitored. Monitoring must be started later
    # separately.
    #
    # @return [ Array<Server> ] Servers actually added to the cluster.
    #   This is the set of servers on which monitoring should be started.
    def add_servers_from_desc(updated_desc)
      added_servers = []
      %w(hosts passives arbiters).each do |m|
        updated_desc.send(m).each do |address_str|
          if server = cluster.add(address_str, monitor: false)
            added_servers << server
          end
        end
      end

      verify_invariants

      added_servers
    end

    # Removes servers from the topology which are not present in the
    # given server description (which is supposed to have come from a
    # good primary).
    def remove_servers_not_in_desc(updated_desc)
      updated_desc_address_strs = %w(hosts passives arbiters).map do |m|
        updated_desc.send(m)
      end.flatten
      servers_list.each do |server|
        unless updated_desc_address_strs.include?(address_str = server.address.to_s)
          updated_host = updated_desc.address.to_s
          if updated_desc.me && updated_desc.me != updated_host
            updated_host += " (self-identified as #{updated_desc.me})"
          end
          log_warn(
            "Removing server #{address_str} because it is not in hosts reported by primary " +
            "#{updated_host}. Reported hosts are: " +
            updated_desc.hosts.join(', ')
          )
          do_remove(address_str)
        end
      end
    end

    # Removes the server whose description we are processing from the
    # topology.
    def remove
      publish_description_change_event
      do_remove(updated_desc.address.to_s)
    end

    # Removes specified server from topology and warns if the topology ends
    # up with an empty server list as a result
    def do_remove(address_str)
      servers = cluster.remove(address_str, disconnect: false)
      servers.each do |server|
        # Clear the description so that the server is marked as unknown.
        server.clear_description

        # We need to publish server closed event here, but we cannot close
        # the server because it could be the server owning the monitor in
        # whose thread this flow is presently executing, in which case closing
        # the server can terminate the thread and leave SDAM processing
        # incomplete. Thus we have to remove the server from the cluster,
        # publish the event, but do not call disconnect on the server until
        # the very end when all processing has completed.
        publish_sdam_event(
          Mongo::Monitoring::SERVER_CLOSED,
          Mongo::Monitoring::Event::ServerClosed.new(server.address, cluster.topology)
        )
      end
      @servers_to_disconnect += servers
      if servers_list.empty?
        log_warn(
          "Topology now has no servers - this is likely a misconfiguration of the cluster and/or the application"
        )
      end
    end

    def publish_description_change_event
      # This method may be invoked when server description definitely changed
      # but prior to the topology getting updated. Therefore we check both
      # server description changes and overall topology changes. When this
      # method is called at the end of SDAM flow as part of "commit changes"
      # step, server description change is incorporated into the topology
      # change.
      unless @server_description_changed || topology_effectively_changed?
        return
      end

      # updated_desc here may not be the description we received from
      # the server - in case of a stale primary, the server reported itself
      # as being a primary but updated_desc here will be unknown.

      # We used to not notify on Unknown -> Unknown server changes.
      # Technically these are valid state changes (or at least as valid as
      # other server description changes when the description has not
      # changed meaningfully but the events are still published).
      # The current version of the driver notifies on Unknown -> Unknown
      # transitions.

      # Avoid dispatching events when updated description is the same as
      # previous description. This allows this method to be called multiple
      # times in the flow when the events should be published, without
      # worrying about whether there are any unpublished changes.
      if updated_desc.object_id == previous_desc.object_id
        return
      end

      publish_sdam_event(
        ::Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED,
        ::Mongo::Monitoring::Event::ServerDescriptionChanged.new(
          updated_desc.address,
          topology,
          previous_desc,
          updated_desc,
          awaited: awaited?,
        )
      )
      @previous_desc = updated_desc
      @need_topology_changed_event = true
    end

    # Publishes server description changed events, updates topology on
    # the cluster and publishes topology changed event, as needed
    # based on operations performed during SDAM flow processing.
    def commit_changes
      # The application-visible sequence of events should be as follows:
      #
      # 1. Description change for the server which we are processing;
      # 2. Topology change, if any;
      # 3. Description changes for other servers, if any.
      #
      # The tricky part here is that the server description changes are
      # not all processed together.

      publish_description_change_event
      start_pool_if_data_bearing

      topology_changed_event_published = false
      if !topology.equal?(cluster.topology) || @need_topology_changed_event
        # We are about to publish topology changed event.
        # Recreate the topology instance to get its server descriptions
        # up to date.
        @topology = topology.class.new(topology.options, topology.monitoring, cluster)
        # This sends the SDAM event
        cluster.update_topology(topology)
        topology_changed_event_published = true
        @need_topology_changed_event = false
      end

      # If a server description changed, topology description change event
      # must be published with the previous and next topologies being of
      # the same type, unless we already published topology change event
      if topology_changed_event_published
        return
      end

      if updated_desc.unknown? && previous_desc.unknown?
        return
      end
      if updated_desc.object_id == previous_desc.object_id
        return
      end

      unless topology_effectively_changed?
        return
      end

      # If we are here, there has been a change in the server descriptions
      # in our topology, but topology class has not changed.
      # Publish the topology changed event and recreate the topology to
      # get the new list of server descriptions into it.
      @topology = topology.class.new(topology.options, topology.monitoring, cluster)
      # This sends the SDAM event
      cluster.update_topology(topology)
    end

    def disconnect_servers
      while server = @servers_to_disconnect.shift
        if server.connected?
          # Do not publish server closed event, as this was already done
          server.disconnect!
        end
      end
    end

    # If the server being processed is identified as data bearing, creates the
    # server's connection pool so it can start populating
    def start_pool_if_data_bearing
      return if !updated_desc.data_bearing?

      servers_list.each do |server|
        if server.address == @updated_desc.address
          server.pool
        end
      end
    end

    # Checks if the cluster has a primary, and if not, transitions the topology
    # to ReplicaSetNoPrimary. Topology must be ReplicaSetWithPrimary when
    # invoking this method.
    def check_if_has_primary
      unless topology.replica_set?
        raise ArgumentError, "check_if_has_primary should only be called when topology is replica set, but it is #{topology.class.name.sub(/.*::/, '')}"
      end

      primary = servers_list.detect do |server|
        # A primary with the wrong set name is not a primary
        server.primary? && server.description.replica_set_name == topology.replica_set_name
      end
      unless primary
        @topology = Topology::ReplicaSetNoPrimary.new(
          topology.options, topology.monitoring, self)
      end
    end

    # Whether updated_desc is for a stale primary.
    def stale_primary?
      if updated_desc.max_wire_version >= 17
        if updated_desc.election_id.nil? && !topology.max_election_id.nil?
          return true
        end
        if updated_desc.election_id && topology.max_election_id && updated_desc.election_id < topology.max_election_id
          return true
        end
        if updated_desc.election_id == topology.max_election_id
          if updated_desc.set_version.nil? && !topology.max_set_version.nil?
            return true
          end
          if updated_desc.set_version && topology.max_set_version && updated_desc.set_version < topology.max_set_version
            return true
          end
        end
      else
        if updated_desc.election_id && updated_desc.set_version
          if topology.max_set_version && topology.max_election_id &&
              (updated_desc.set_version < topology.max_set_version ||
                  (updated_desc.set_version == topology.max_set_version &&
                      updated_desc.election_id < topology.max_election_id))
            return true
          end
        end
      end
      false
    end

    # Returns whether the server whose description this flow processed
    # was not previously unknown, and is now. Used to decide, in particular,
    # whether to clear the server's connection pool.
    def became_unknown?
      updated_desc.unknown? && !original_desc.unknown?
    end

    # Returns whether topology meaningfully changed as a result of running
    # SDAM flow.
    #
    # The spec defines topology equality through equality of topology types
    # and server descriptions in each topology; this definition is not usable
    # by us because our topology objects do not hold server descriptions and
    # are instead "live". Thus we have to store the full list of server
    # descriptions at the beginning of SDAM flow and compare them to the
    # current ones.
    def topology_effectively_changed?
      unless topology.equal?(cluster.topology)
        return true
      end

      server_descriptions = servers_list.map do |server|
        [server.address.to_s, server.description]
      end

      @previous_server_descriptions != server_descriptions
    end

    def verify_invariants
      if Mongo::Lint.enabled?
        if cluster.topology.single?
          if cluster.servers_list.length > 1
            raise Mongo::Error::LintError, "Trying to create a single topology with multiple servers: #{cluster.servers_list}"
          end
        end
      end
    end
  end
end
