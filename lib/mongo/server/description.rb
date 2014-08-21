# Copyright (C) 2009-2014 MongoDB, Inc.
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

require 'mongo/server/description/inspection'

module Mongo
  class Server

    # Represents a description of the server, populated by the result of the
    # ismaster command.
    #
    # @since 2.0.0
    class Description

      # Constant for reading arbiter info from config.
      #
      # @since 2.0.0
      ARBITER = 'arbiterOnly'.freeze

      # Constant for reading arbiters info from config.
      #
      # @since 2.0.0
      ARBITERS = 'arbiters'.freeze

      # Constant for reading hidden info from config.
      #
      # @since 2.0.0
      HIDDEN = 'hidden'.freeze

      # Constant for reading hosts info from config.
      #
      # @since 2.0.0
      HOSTS = 'hosts'.freeze

      # Constant for the key for the message value.
      #
      # @since 2.0.0
      MESSAGE = 'msg'.freeze

      # Constant for the message that indicates a sharded cluster.
      #
      # @since 2.0.0
      MONGOS_MESSAGE = 'isdbgrid'.freeze

      # Constant for determining ghost servers.
      #
      # @since 2.0.0
      REPLICA_SET = 'isreplicaset'.freeze

      # Static list of inspections that are performed on the result of an
      # ismaster command in order to generate the appropriate events for the
      # changes.
      #
      # @since 2.0.0
      INSPECTIONS = [
        Inspection::ServerAdded,
        Inspection::ServerRemoved
      ].freeze

      # Constant for reading max bson size info from config.
      #
      # @since 2.0.0
      MAX_BSON_OBJECT_SIZE = 'maxBsonObjectSize'.freeze

      # Constant for reading max message size info from config.
      #
      # @since 2.0.0
      MAX_MESSAGE_BYTES = 'maxMessageSizeBytes'.freeze

      # Constant for the max wire version.
      #
      # @since 2.0.0
      MAX_WIRE_VERSION = 'maxWireVersion'.freeze

      # Constant for min wire version.
      #
      # @since 2.0.0
      MIN_WIRE_VERSION = 'minWireVersion'.freeze

      # Constant for reading max write batch size.
      #
      # @since 2.0.0
      MAX_WRITE_BATCH_SIZE = 'maxWriteBatchSize'.freeze

      # The legacy wire protocol version.
      #
      # @since 2.0.0
      LEGACY_WIRE_VERSION = 0.freeze

      # Constant for reading passive info from config.
      #
      # @since 2.0.0
      PASSIVE = 'passive'.freeze

      # Constant for reading primary info from config.
      #
      # @since 2.0.0
      PRIMARY = 'ismaster'.freeze

      # Constant for reading secondary info from config.
      #
      # @since 2.0.0
      SECONDARY = 'secondary'.freeze

      # Constant for reading replica set name info from config.
      #
      # @since 2.0.0
      SET_NAME = 'setName'.freeze

      # The minimum version of the wire protocol to support write commands.
      #
      # @since 2.0.0
      WRITE_COMMAND_WIRE_VERSION = 2.freeze

      # @return [ Hash ] The actual result from the isnamster command.
      attr_reader :config

      # @return [ Float ] The time the ismaster call took to complete.
      attr_reader :round_trip_time

      # @return [ Mongo::Server ] server Needed to fire events.
      attr_reader :server

      # @return [ Symbol ] The type of server this description represents.
      attr_accessor :server_type

      # Will return true if the server is an arbiter.
      #
      # @example Is the server an arbiter?
      #   description.arbiter?
      #
      # @return [ true, false ] If the server is an arbiter.
      #
      # @since 2.0.0
      def arbiter?
        !!config[ARBITER] && !replica_set_name.nil?
      end

      # Get a list of all arbiters in the replica set.
      #
      # @example Get the arbiters in the replica set.
      #   description.arbiters
      #
      # @return [ Array<String> ] The arbiters in the set.
      #
      # @since 2.0.0
      def arbiters
        config[ARBITERS] || []
      end

      # Is the server a ghost in a replica set?
      #
      # @example Is the server a ghost?
      #   description.ghost?
      #
      # @return [ true, false ] If the server is a ghost.
      #
      # @since 2.0.0
      def ghost?
        !!config[REPLICA_SET]
      end

      # Will return true if the server is hidden.
      #
      # @example Is the server hidden?
      #   description.hidden?
      #
      # @return [ true, false ] If the server is hidden.
      #
      # @since 2.0.0
      def hidden?
        !!config[HIDDEN]
      end

      # Get a list of all servers in the replica set.
      #
      # @example Get the servers in the replica set.
      #   description.hosts
      #
      # @return [ Array<String> ] The servers in the set.
      #
      # @since 2.0.0
      def hosts
        config[HOSTS] || []
      end

      # Instantiate the new server description from the result of the ismaster
      # command.
      #
      # @example Instantiate the new description.
      #   Description.new(result)
      #
      # @param [ Hash ] config The result of the ismaster command.
      #
      # @since 2.0.0
      def initialize(server, config = {}, round_trip_time = 0)
        @server = server
        @config = config
        @round_trip_time = round_trip_time
      end

      # Get the max BSON object size for this server version.
      #
      # @example Get the max BSON object size.
      #   description.max_bson_object_size
      #
      # @return [ Integer ] The maximum object size in bytes.
      #
      # @since 2.0.0
      def max_bson_object_size
        config[MAX_BSON_OBJECT_SIZE]
      end

      # Get the max message size for this server version.
      #
      # @example Get the max message size.
      #   description.max_message_size
      #
      # @return [ Integer ] The maximum message size in bytes.
      #
      # @since 2.0.0
      def max_message_size
        config[MAX_MESSAGE_BYTES]
      end

      # Get the maximum batch size for writes.
      #
      # @example Get the max batch size.
      #   config.max_write_batch_size
      #
      # @return [ Integer ] The max batch size.
      #
      # @since 2.0.0
      def max_write_batch_size
        config[MAX_WRITE_BATCH_SIZE]
      end

      # Get the maximum wire version.
      #
      # @example Get the max wire version.
      #   config.max_wire_version
      #
      # @return [ Integer ] The max wire version supported.
      #
      # @since 2.0.0
      def max_wire_version
        config[MAX_WIRE_VERSION] || LEGACY_WIRE_VERSION
      end

      # Get the minimum wire version.
      #
      # @example Get the min wire version.
      #   config.min_wire_version
      #
      # @return [ Integer ] The min wire version supported.
      #
      # @since 2.0.0
      def min_wire_version
        config[MIN_WIRE_VERSION] || LEGACY_WIRE_VERSION
      end

      # Is the server a mongos?
      #
      # @example Is the server a mongos?
      #   description.mongos?
      #
      # @return [ true, false ] If the server is a mongos.
      #
      # @since 2.0.0
      def mongos?
        config[MESSAGE] == MONGOS_MESSAGE
      end

      # Will return true if the server is passive.
      #
      # @example Is the server passive?
      #   description.passive?
      #
      # @return [ true, false ] If the server is passive.
      #
      # @since 2.0.0
      def passive?
        !!config[PASSIVE]
      end

      # Will return true if the server is a primary.
      #
      # @example Is the server a primary?
      #   description.primary?
      #
      # @return [ true, false ] If the server is a primary.
      #
      # @since 2.0.0
      def primary?
        !!config[PRIMARY] && !replica_set_name.nil?
      end

      # Get the name of the replica set the server belongs to, returns nil if
      # none.
      #
      # @example Get the replica set name.
      #   description.replica_set_name
      #
      # @return [ String, nil ] The name of the replica set.
      #
      # @since 2.0.0
      def replica_set_name
        config[SET_NAME]
      end

      # Will return true if the server is a secondary.
      #
      # @example Is the server a secondary?
      #   description.secondary?
      #
      # @return [ true, false ] If the server is a secondary.
      #
      # @since 2.0.0
      def secondary?
        !!config[SECONDARY] && !replica_set_name.nil?
      end

      # Is this server a standalone server?
      #
      # @example Is the server standalone?
      #   description.standalone?
      #
      # @return [ true, false ] If the server is standalone.
      #
      # @since 2.0.0
      def standalone?
        replica_set_name.nil? && !mongos? && !ghost? && !unknown?
      end

      # Is the server description currently unknown?
      #
      # @example Is the server description unknown?
      #   description.unknown?
      #
      # @return [ true, false ] If the server description is unknown.
      #
      # @since 2.0.0
      def unknown?
        config.empty?
      end

      # Update this description with a new description. Will fire the
      # necessary events depending on what has changed from the old description
      # to the new one.
      #
      # @example Update the description with the new config.
      #   description.update!({ "ismaster" => false })
      #
      # @note This modifies the state of the description.
      #
      # @param [ Hash ] new_config The new configuration.
      #
      # @return [ Description ] The updated description.
      #
      # @since 2.0.0
      def update!(new_config, round_trip_time)
        INSPECTIONS.each do |inspection|
          inspection.run(self, Description.new(server, new_config))
        end
        @config = new_config
        @round_trip_time = round_trip_time
        self
      end

      # Is the server able to perform write commands.
      #
      # @example Is the server write command enabled?
      #   description.write_command_enabled?
      #
      # @return [ true, false ] If the server is write command enabled.
      #
      # @since 2.0.0
      def write_command_enabled?
        max_wire_version >= WRITE_COMMAND_WIRE_VERSION
      end
    end
  end
end
