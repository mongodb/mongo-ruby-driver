# Copyright (C) 2014-2017 MongoDB, Inc.
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

require 'mongo/server/description/features'
require 'mongo/server/description/inspector'

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

      # Constant for the lastWrite subdocument.
      #
      # @since 2.4.0
      LAST_WRITE = 'lastWrite'.freeze

      # Constant for the lastWriteDate field in the lastWrite subdocument.
      #
      # @since 2.4.0
      LAST_WRITE_DATE = 'lastWriteDate'.freeze

      # Constant for reading the me field.
      #
      # @since 2.1.0
      ME = 'me'.freeze

      # Default max write batch size.
      #
      # @since 2.0.0
      DEFAULT_MAX_WRITE_BATCH_SIZE = 1000.freeze

      # The legacy wire protocol version.
      #
      # @since 2.0.0
      LEGACY_WIRE_VERSION = 0.freeze

      # Constant for reading passive info from config.
      #
      # @since 2.0.0
      PASSIVE = 'passive'.freeze

      # Constant for reading the passive server list.
      #
      # @since 2.0.0
      PASSIVES = 'passives'.freeze

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

      # Constant for reading tags info from config.
      #
      # @since 2.0.0
      TAGS = 'tags'.freeze

      # Constant for reading electionId info from config.
      #
      # @since 2.1.0
      ELECTION_ID = 'electionId'.freeze

      # Constant for reading setVersion info from config.
      #
      # @since 2.2.2
      SET_VERSION = 'setVersion'.freeze

      # Constant for reading localTime info from config.
      #
      # @since 2.1.0
      LOCAL_TIME = 'localTime'.freeze

      # Fields to exclude when comparing two descriptions.
      #
      # @since 2.0.6
      EXCLUDE_FOR_COMPARISON = [ LOCAL_TIME, LAST_WRITE ].freeze

      # @return [ Address ] address The server's address.
      attr_reader :address

      # @return [ Hash ] The actual result from the ismaster command.
      attr_reader :config

      # @return [ Features ] features The features for the server.
      attr_reader :features

      # @return [ Float ] The moving average time the ismaster call took to complete.
      attr_reader :average_round_trip_time

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
        @arbiters ||= (config[ARBITERS] || []).map { |s| s.downcase }
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
        @hosts ||= (config[HOSTS] || []).map { |s| s.downcase }
      end

      # Instantiate the new server description from the result of the ismaster
      # command.
      #
      # @example Instantiate the new description.
      #   Description.new(address, { 'ismaster' => true }, 0.5)
      #
      # @param [ Address ] address The server address.
      # @param [ Hash ] config The result of the ismaster command.
      # @param [ Float ] average_round_trip_time The moving average time (sec) the ismaster
      #   call took to complete.
      #
      # @since 2.0.0
      def initialize(address, config = {}, average_round_trip_time = 0)
        @address = address
        @config = config
        @features = Features.new(wire_versions)
        @average_round_trip_time = average_round_trip_time
      end

      # Inspect the server description.
      #
      # @example Inspect the server description
      #   description.inspect
      #
      # @return [ String ] The inspection.
      #
      # @since 2.0.0
      def inspect
        "#<Mongo::Server:Description:0x#{object_id} config=#{config} average_round_trip_time=#{average_round_trip_time}>"
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
      #   description.max_write_batch_size
      #
      # @return [ Integer ] The max batch size.
      #
      # @since 2.0.0
      def max_write_batch_size
        config[MAX_WRITE_BATCH_SIZE] || DEFAULT_MAX_WRITE_BATCH_SIZE
      end

      # Get the maximum wire version.
      #
      # @example Get the max wire version.
      #   description.max_wire_version
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
      #   description.min_wire_version
      #
      # @return [ Integer ] The min wire version supported.
      #
      # @since 2.0.0
      def min_wire_version
        config[MIN_WIRE_VERSION] || LEGACY_WIRE_VERSION
      end

      # Get the me field value.
      #
      # @example Get the me field value.
      #   description.me
      #
      # @return [ String ] The me field.
      #
      # @since 2.1.0
      def me
        config[ME]
      end

      # Get the tags configured for the server.
      #
      # @example Get the tags.
      #   description.tags
      #
      # @return [ Hash ] The tags of the server.
      #
      # @since 2.0.0
      def tags
        config[TAGS] || {}
      end

      # Get the electionId from the config.
      #
      # @example Get the electionId.
      #   description.election_id
      #
      # @return [ BSON::ObjectId ] The election id.
      #
      # @since 2.1.0
      def election_id
        config[ELECTION_ID]
      end

      # Get the setVersion from the config.
      #
      # @example Get the setVersion.
      #   description.set_version
      #
      # @return [ Integer ] The set version.
      #
      # @since 2.2.2
      def set_version
        config[SET_VERSION]
      end

      # Get the lastWriteDate from the lastWrite subdocument in the config.
      #
      # @example Get the lastWriteDate value.
      #   description.last_write_date
      #
      # @return [ Time ] The last write date.
      #
      # @since 2.4.0
      def last_write_date
        config[LAST_WRITE][LAST_WRITE_DATE] if config[LAST_WRITE]
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

      # Is the description of type other.
      #
      # @example Is the description of type other.
      #   description.other?
      #
      # @return [ true, false ] If the description is other.
      #
      # @since 2.0.0
      def other?
        (!primary? && !secondary? && !passive? && !arbiter?) ||
          (hidden? && !replica_set_name.nil?)
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

      # Get a list of the passive servers in the cluster.
      #
      # @example Get the passives.
      #   description.passives
      #
      # @return [ Array<String> ] The list of passives.
      #
      # @since 2.0.0
      def passives
        @passives ||= (config[PASSIVES] || []).map { |s| s.downcase }
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

      # Get a list of all servers known to the cluster.
      #
      # @example Get all servers.
      #   description.servers
      #
      # @return [ Array<String> ] The list of all servers.
      #
      # @since 2.0.0
      def servers
        hosts + arbiters + passives
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

      # Returns the server type as a symbol.
      #
      # @example Get the server type.
      #   description.server_type
      #
      # @return [ Symbol ] The server type.
      #
      # @since 2.4.0
      def server_type
        return :arbiter if arbiter?
        return :ghost if ghost?
        return :sharded if mongos?
        return :primary if primary?
        return :secondary if secondary?
        return :standalone if standalone?
        :unknown
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
        config.empty? || (config[Operation::Result::OK] &&
                            config[Operation::Result::OK] != 1)
      end

      # A result from another server's ismaster command before this server has
      # refreshed causes the need for this description to become unknown before
      # the next refresh.
      #
      # @example Force an unknown state.
      #   description.unknown!
      #
      # @return [ true ] Always true.
      #
      # @since 2.0.0
      def unknown!
        @config = {} and true
      end

      # Get the range of supported wire versions for the server.
      #
      # @example Get the wire version range.
      #   description.wire_versions
      #
      # @return [ Range ] The wire version range.
      #
      # @since 2.0.0
      def wire_versions
        min_wire_version..max_wire_version
      end

      # Is this description from the given server.
      #
      # @example Check if the description is from a given server.
      #   description.is_server?(server)
      #
      # @return [ true, false ] If the description is from the server.
      #
      # @since 2.0.6
      def is_server?(server)
        address == server.address
      end

      # Is a server included in this description's list of servers.
      #
      # @example Check if a server is in the description list of servers.
      #   description.lists_server?(server)
      #
      # @return [ true, false ] If a server is in the description's list
      #   of servers.
      #
      # @since 2.0.6
      def lists_server?(server)
        servers.include?(server.address.to_s)
      end

      # Does this description correspond to a replica set member.
      #
      # @example Check if the description is from a replica set member.
      #   description.replica_set_member?
      #
      # @return [ true, false ] If the description is from a replica set
      #   member.
      #
      # @since 2.0.6
      def replica_set_member?
        !(standalone? || mongos?)
      end

      # Check if there is a mismatch between the address host and the me field.
      #
      # @example Check if there is a mismatch.
      #   description.me_mismatch?
      #
      # @return [ true, false ] If there is a mismatch between the me field and the address host.
      #
      # @since 2.0.6
      def me_mismatch?
        !!(address.to_s != me if me)
      end

      # Check equality of two descriptions.
      #
      # @example Check description equality.
      #   description == other
      #
      # @param [ Object ] other The other description.
      #
      # @return [ true, false ] Whether the objects are equal.
      #
      # @since 2.0.6
      def ==(other)
        return false if self.class != other.class
        return false if unknown? || other.unknown?
        compare_config(other)
      end
      alias_method :eql?, :==

      private

      def compare_config(other)
        config.keys.all? do |k|
          config[k] == other.config[k] || EXCLUDE_FOR_COMPARISON.include?(k)
        end
      end
    end
  end
end
