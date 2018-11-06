# Copyright (C) 2018 MongoDB, Inc.
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
  class Cluster
    module Topology

      # Defines behavior common to all topologies.
      #
      # @since 2.7.0
      class Base
        extend Forwardable
        include Loggable
        include Monitoring::Publishable

        # Initialize the topology with the options.
        #
        # @param [ Hash ] options The options.
        # @param [ Monitoring ] monitoring The monitoring.
        # @param [ Cluster ] cluster The cluster.
        #
        # @option options [ Symbol ] :replica_set Name of the replica set to
        #   connect to. Can be left blank (either nil or the empty string are
        #   accepted) to discover the name from the cluster. If the addresses
        #   belong to different replica sets there is no guarantee which
        #   replica set is selected - in particular, the driver may choose
        #   the replica set name of a secondary if it returns its response
        #   prior to a primary belonging to a different replica set.
        #   This option can only be specified when instantiating a replica
        #   set topology.
        # @option options [ BSON::ObjectId ] :max_election_id Max election id
        #   per the SDAM specification.
        #   This option can only be specified when instantiating a replica
        #   set topology.
        # @option options [ Integer ] :max_set_version Max set version
        #   per the SDAM specification.
        #   This option can only be specified when instantiating a replica
        #   set topology.
        #
        # @since 2.7.0
        # @api private
        def initialize(options, monitoring, cluster)
          options = validate_options(options)

          @options = options
          @monitoring = monitoring
          @cluster = cluster
          # The list of server descriptions is simply fixed at the time of
          # topology creation. If server description change later, a
          # new topology instance should be created.
          servers = cluster.servers_list
          @server_descriptions = ServerDescriptionList.new(
            servers.map(&:description))

          begin
            servers.each do |server|
              server.check_driver_support!
            end
          rescue Error::UnsupportedFeatures => e
            @compatible = false
            @compatibility_error = e
          else
            @compatible = true
          end
        end

        # @return [ Hash ] options The options.
        attr_reader :options

        # @return [ Cluster ] The cluster.
        # @api private
        attr_reader :cluster
        private :cluster

        # @return [ Array<String> ] addresses Server addresses.
        def addresses
          cluster.addresses.map(&:seed)
        end

        # @return [ monitoring ] monitoring the monitoring.
        attr_reader :monitoring

        # @return [ ServerDescriptionList ] server_descriptions The list of
        #   server descriptions.
        #
        # @since 2.7.0
        attr_reader :server_descriptions

        # @return [ true|false ] compatible Whether topology is compatible
        #   with the driver.
        #
        # @since 2.7.0
        def compatible?
          @compatible
        end

        # @return [ Exception ] compatibility_error If topology is incompatible
        #   with the driver, an exception with information regarding the incompatibility.
        #   If topology is compatible with the driver, nil.
        #
        # @since 2.7.0
        attr_reader :compatibility_error

        # The largest electionId ever reported by a primary.
        # May be nil.
        #
        # @return [ BSON::ObjectId ] The election id.
        #
        # @since 2.7.0
        def max_election_id
          options[:max_election_id]
        end

        # The largest setVersion ever reported by a primary.
        # May be nil.
        #
        # @return [ Integer ] The set version.
        #
        # @since 2.7.0
        def max_set_version
          options[:max_set_version]
        end

        # @api private
        def new_max_election_id(description)
          if description.election_id &&
              (max_election_id.nil? ||
                  description.election_id > max_election_id)
            description.election_id
          else
            max_election_id
          end
        end

        # @api private
        def new_max_set_version(description)
          if description.set_version &&
              (max_set_version.nil? ||
                  description.set_version > max_set_version)
            description.set_version
          else
            max_set_version
          end
        end

        private

        # Validates and/or transforms options as necessary for the topology.
        #
        # @return [ Hash ] New options
        def validate_options(options)
          options
        end
      end
    end
  end
end
