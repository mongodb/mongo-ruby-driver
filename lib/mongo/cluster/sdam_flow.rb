class Mongo::Cluster
  # @api private
  class SdamFlow
    extend Forwardable

    def initialize(cluster)
      @cluster = cluster
    end

    attr_reader :cluster

    def_delegators :cluster, :servers_list, :topology, :seeds, :replica_set_name,
      :publish_sdam_event, :update_topology,
      :log_warn

    # TODO see about removing this
    def_delegators :cluster, :servers

    # Add hosts in a description to the cluster.
    #
    # @example Add hosts in a description to the cluster.
    #   cluster.add_hosts(description)
    #
    # @param [ Mongo::Server::Description ] description The description.
    #
    # @since 2.0.6
    def add_hosts(description)
      if topology.add_hosts?(description, servers_list)
        description.servers.each { |s| cluster.add(s) }
      end
    end

    # Remove hosts in a description from the cluster.
    #
    # @example Remove hosts in a description from the cluster.
    #   cluster.remove_hosts(description)
    #
    # @param [ Mongo::Server::Description ] description The description.
    #
    # @since 2.0.6
    def remove_hosts(description)
      if topology.remove_hosts?(description)
        servers_list.each do |s|
          cluster.remove(s.address.to_s) if topology.remove_server?(description, s)
        end
      end
    end

    # Elect a primary server from the description that has just changed to a
    # primary.
    #
    # @example Elect a primary server.
    #   cluster.elect_primary!(description)
    #
    # @param [ Server::Description ] description The newly elected primary.
    #
    # @return [ Topology ] The cluster topology.
    #
    # @since 2.0.0
    def elect_primary!(description)
      # Update descriptions - temporary hack until
      # https://jira.mongodb.org/browse/RUBY-1509 is implemented.
      # There are multiple placet that can update descriptions, these should
      # be DRYed to a single one.
      servers = servers_list
      servers.each do |server|
        if server.address == description.address
          server.update_description(description)
        end
      end

      new_topology = nil
      if topology.unknown?
        new_topology = if description.mongos?
          Topology::Sharded.new(topology.options, topology.monitoring, self)
        else
          initialize_replica_set(description, servers)
        end
      elsif topology.replica_set?
        if description.replica_set_name == replica_set_name
          if detect_stale_primary!(description)
            # Since detect_stale_primary! can mutate description,
            # we need another pass of updating descriptions on our servers.
            # https://jira.mongodb.org/browse/RUBY-1509
            servers.each do |server|
              if server.address == description.address
                server.update_description(description)
              end
            end
          else
            # If we had another server marked as primary, mark that one
            # unknown.
            servers.each do |server|
              if server.primary? && server.address != description.address
                server.description.unknown!
              end
            end

            max_election_id = topology.new_max_election_id(description)
            max_set_version = topology.new_max_set_version(description)

            cls = if servers.any?(&:primary?)
              Topology::ReplicaSetWithPrimary
            else
              Topology::ReplicaSetNoPrimary
            end
            new_topology = cls.new(topology.options,
              topology.monitoring,
              self,
              max_election_id,
              max_set_version)
          end
        else
          log_warn(
            "Server #{description.address.to_s} has incorrect replica set name: " +
            "'#{description.replica_set_name}'. The current replica set name is '#{topology.replica_set_name}'."
          )
        end
      end

      if new_topology
        update_topology(new_topology)
        # Even though the topology class selection above already attempts
        # to figure out if the topology has a primary, in some cases
        # we already are in a replica set topology and an additional
        # primary check must be performed here.
        if topology.replica_set?
          check_if_has_primary
        end
      end
    end

    # Handles a change in server description.
    #
    # @param [ Server::Description ] previous_desc Previous server description.
    # @param [ Server::Description ] updated_desc The new description.
    #
    # @api private
    def server_description_changed(previous_desc, updated_desc)
      # When a server description change leads to a topology type change,
      # the topology is changed first (i.e. as visible through SDAM events),
      # with the server being present in the new topology as unknown,
      # and the server is then changed in the new topology to its correct type.
      if updated_desc.standalone? && !previous_desc.standalone?
        # standalone discovered
        if topology.unknown? && seeds.length == 1
          update_topology(
            Topology::Single.new(topology.options, topology.monitoring, self))
        else
          # TODO warn that the discovered standalone is dropped
        end
      end

      if updated_desc != previous_desc && (!updated_desc.unknown? || !previous_desc.unknown?)
        # server description changed
        # transitioning from an unknown to another unknown does not
        # generate sdam events, apparently
        handle_server_description_changed(previous_desc, updated_desc)
      end

      if updated_desc.primary? && !previous_desc.primary? ||
        updated_desc.mongos? && !previous_desc.mongos?
      then
        elect_primary!(updated_desc)
      end

      if !updated_desc.unknown? && previous_desc.unknown?
        if topology.unknown? || topology.single?
          publish_sdam_event(::Mongo::Monitoring::TOPOLOGY_CHANGED,
            ::Mongo::Monitoring::Event::TopologyChanged.new(topology, topology))
        end
      end
    end

    # Handles a change in server description.
    #
    # @param [ Server::Description ] previous_desc Previous server description.
    # @param [ Server::Description ] updated_desc The new description.
    #
    # @api private
    def handle_server_description_changed(previous_desc, updated_desc)
      # https://jira.mongodb.org/browse/RUBY-1509
      servers_list.each do |server|
        if server.address == updated_desc.address
          server.update_description(updated_desc)
        end
      end

      publish_sdam_event(
        ::Mongo::Monitoring::SERVER_DESCRIPTION_CHANGED,
        ::Mongo::Monitoring::Event::ServerDescriptionChanged.new(
          updated_desc.address,
          topology,
          previous_desc,
          updated_desc,
        )
      )

      add_hosts(updated_desc)
      remove_hosts(updated_desc)

      if updated_desc.ghost? && !topology.is_a?(Topology::Sharded)
        # https://jira.mongodb.org/browse/RUBY-1509
        servers.each do |server|
          if server.address == updated_desc.address
            server.update_description(updated_desc)
          end
        end
      end

      if topology.is_a?(::Mongo::Cluster::Topology::Unknown) &&
        updated_desc.replica_set_name &&
        updated_desc.replica_set_name != ''
      then
        transition_to_replica_set(updated_desc)
=begin pending further refactoring
      elsif topology.is_a?(Cluster::Topology::ReplicaSetWithPrimary) &&
        (updated_desc.unknown? ||
          updated_desc.standalone? ||
          updated_desc.mongos? ||
          updated_desc.ghost?)
      then
        # here the unknown server is already removed from the topology
        check_if_has_primary
=end
      end

      # This check may be invoked in more cases than is necessary per
      # the spec, but currently it is hard to know exactly when it should
      # be invoked and the commented out condition above does not catch
      # all cases. In any event check_if_has_primary is harmless if
      # the topology does not transition.
      if topology.replica_set?
        check_if_has_primary
      end
    end

    # Checks if the cluster has a primary, and if not, transitions the topology
    # to ReplicaSetNoPrimary. Topology must be ReplicaSetWithPrimary when
    # invoking this method.
    #
    # @api private
    def check_if_has_primary
      unless topology.replica_set?
        raise ArgumentError, 'check_if_has_primary should only be called when topology is replica set'
      end

      primary = servers.detect do |server|
        # A primary with the wrong set name is not a primary
        server.primary? && server.description.replica_set_name == topology.replica_set_name
      end
      unless primary
        update_topology(Topology::ReplicaSetNoPrimary.new(
          topology.options, topology.monitoring, self,
          topology.max_election_id, topology.max_set_version))
      end
    end

    # Transitions topology from unknown to one of the two replica set
    # topologies, depending on whether the updated description came from
    # a primary. Topology must be Unknown when invoking this method.
    #
    # @param [ Server::Description ] updated_description The changed description.
    #
    # @api private
    def transition_to_replica_set(updated_description)
      new_cls = if updated_description.primary?
        ::Mongo::Cluster::Topology::ReplicaSetWithPrimary
      else
        ::Mongo::Cluster::Topology::ReplicaSetNoPrimary
      end
      update_topology(new_cls.new(
        topology.options.merge(
          replica_set: updated_description.replica_set_name,
        ), topology.monitoring, self))
    end

    # Creates a replica set topology, either having the primary or
    # not, based on description and servers provided.
    # May mutate servers' descriptions.
    #
    # Description must be of a server in the replica set topology, and
    # is used to obtain the replica set name among other things.
    def initialize_replica_set(description, servers)
      servers.each do |server|
        if server.standalone? && server.address != description.address
          server.description.unknown!
        end
      end
      cls = if servers.any?(&:primary?)
        Topology::ReplicaSetWithPrimary
      else
        Topology::ReplicaSetNoPrimary
      end
      cls.new(topology.options.merge(:replica_set => description.replica_set_name),
        topology.monitoring, self)
    end

    # Checks whether description is for a stale primary, and if so,
    # changes the description to be unknown.
    def detect_stale_primary!(description)
      if description.election_id && description.set_version
        if topology.max_set_version && topology.max_election_id &&
            (description.set_version < topology.max_set_version ||
                (description.set_version == topology.max_set_version &&
                    description.election_id < topology.max_election_id))
          description.unknown!
        end
      end
    end

  end
end
