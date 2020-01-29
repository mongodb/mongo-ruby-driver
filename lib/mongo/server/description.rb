# Copyright (C) 2014-2019 MongoDB, Inc.
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

module Mongo
  class Server

    # Represents a description of the server, populated by the result of the
    # ismaster command.
    #
    # Note: Unknown servers do not have wire versions, but for legacy reasons
    # we return 0 for min_wire_version and max_wire_version of any server that does
    # not have them. Presently the driver sometimes constructs commands when the
    # server is unknown, so references to min_wire_version and max_wire_version
    # should not be nil. When driver behavior is changed
    # (https://jira.mongodb.org/browse/RUBY-1805), this may no longer be necessary.
    #
    # @since 2.0.0
    class Description

      # Constant for reading arbiter info from config.
      #
      # @since 2.0.0
      # @deprecated
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
      # @deprecated
      MESSAGE = 'msg'.freeze

      # Constant for the message that indicates a sharded cluster.
      #
      # @since 2.0.0
      # @deprecated
      MONGOS_MESSAGE = 'isdbgrid'.freeze

      # Constant for determining ghost servers.
      #
      # @since 2.0.0
      # @deprecated
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
      # @deprecated Will be removed in 3.0.
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
      # @deprecated
      PRIMARY = 'ismaster'.freeze

      # Constant for reading primary host field from config.
      #
      # @since 2.5.0
      PRIMARY_HOST = 'primary'.freeze

      # Constant for reading secondary info from config.
      #
      # @since 2.0.0
      # @deprecated
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

      # Constant for reading operationTime info from config.
      #
      # @since 2.5.0
      OPERATION_TIME = 'operationTime'.freeze

      # Constant for reading logicalSessionTimeoutMinutes info from config.
      #
      # @since 2.5.0
      LOGICAL_SESSION_TIMEOUT_MINUTES = 'logicalSessionTimeoutMinutes'.freeze

      # Fields to exclude when comparing two descriptions.
      #
      # @since 2.0.6
      EXCLUDE_FOR_COMPARISON = [ LOCAL_TIME,
                                 LAST_WRITE,
                                 OPERATION_TIME,
                                 Operation::CLUSTER_TIME ].freeze

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
      def initialize(address, config = {}, average_round_trip_time = nil)
        @address = address
        @config = config
        @features = Features.new(wire_versions, me || @address.to_s)
        @average_round_trip_time = average_round_trip_time
        @last_update_time = Time.now.dup.freeze

        if Mongo::Lint.enabled?
          # prepopulate cache instance variables
          hosts
          arbiters
          passives

          freeze
        end
      end

      # @return [ Address ] address The server's address.
      attr_reader :address

      # @return [ Hash ] The actual result from the ismaster command.
      attr_reader :config

      # @return [ Features ] features The features for the server.
      def features
        @features
      end

      # @return [ Float ] The moving average time the ismaster call took to complete.
      attr_reader :average_round_trip_time

      # Returns whether this server is an arbiter, per the SDAM spec.
      #
      # @example Is the server an arbiter?
      #   description.arbiter?
      #
      # @return [ true, false ] If the server is an arbiter.
      #
      # @since 2.0.0
      def arbiter?
        ok? &&
        config['arbiterOnly'] == true &&
        !!config['setName']
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

      # Whether this server is a ghost, per the SDAM spec.
      #
      # @example Is the server a ghost?
      #   description.ghost?
      #
      # @return [ true, false ] If the server is a ghost.
      #
      # @since 2.0.0
      def ghost?
        ok? &&
        config['isreplicaset'] == true
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
        ok? && !!config[HIDDEN]
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

      # Get the maximum wire version. Defaults to zero.
      #
      # @example Get the max wire version.
      #   description.max_wire_version
      #
      # @return [ Integer ] The max wire version supported.
      #
      # @since 2.0.0
      def max_wire_version
        config[MAX_WIRE_VERSION] || 0
      end

      # Get the minimum wire version. Defaults to zero.
      #
      # @example Get the min wire version.
      #   description.min_wire_version
      #
      # @return [ Integer ] The min wire version supported.
      #
      # @since 2.0.0
      def min_wire_version
        config[MIN_WIRE_VERSION] || 0
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

      # Get the logicalSessionTimeoutMinutes from the config.
      #
      # @example Get the logicalSessionTimeoutMinutes value in minutes.
      #   description.logical_session_timeout
      #
      # @return [ Integer, nil ] The logical session timeout in minutes.
      #
      # @since 2.5.0
      def logical_session_timeout
        config[LOGICAL_SESSION_TIMEOUT_MINUTES] if config[LOGICAL_SESSION_TIMEOUT_MINUTES]
      end

      # Returns whether this server is a mongos, per the SDAM spec.
      #
      # @example Is the server a mongos?
      #   description.mongos?
      #
      # @return [ true, false ] If the server is a mongos.
      #
      # @since 2.0.0
      def mongos?
        ok? && config['msg'] == 'isdbgrid'
      end

      # Returns whether the server is an other, per the SDAM spec.
      #
      # @example Is the description of type other.
      #   description.other?
      #
      # @return [ true, false ] If the description is other.
      #
      # @since 2.0.0
      def other?
        # The SDAM spec is slightly confusing on what "other" means,
        # but it's referred to it as "RSOther" which means a non-RS member
        # cannot be "other".
        ok? &&
        !!config['setName'] && (
          config['hidden'] == true ||
          !primary? && !secondary? && !arbiter?
        )
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
        ok? && !!config[PASSIVE]
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

      # Get the address of the primary host.
      #
      # @example Get the address of the primary.
      #   description.primary_host
      #
      # @return [ String | nil ] The address of the primary.
      #
      # @since 2.6.0
      def primary_host
        config[PRIMARY_HOST] && config[PRIMARY_HOST].downcase
      end

      # Returns whether this server is a primary, per the SDAM spec.
      #
      # @example Is the server a primary?
      #   description.primary?
      #
      # @return [ true, false ] If the server is a primary.
      #
      # @since 2.0.0
      def primary?
        ok? &&
        config['ismaster'] == true &&
        !!config['setName']
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

      # Returns whether this server is a secondary, per the SDAM spec.
      #
      # @example Is the server a secondary?
      #   description.secondary?
      #
      # @return [ true, false ] If the server is a secondary.
      #
      # @since 2.0.0
      def secondary?
        ok? &&
        config['secondary'] == true &&
        !!config['setName']
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
        return :other if other?
        :unknown
      end

      # Returns whether this server is a standalone, per the SDAM spec.
      #
      # @example Is the server standalone?
      #   description.standalone?
      #
      # @return [ true, false ] If the server is standalone.
      #
      # @since 2.0.0
      def standalone?
        ok? &&
        config['msg'] != 'isdbgrid' &&
        config['setName'].nil? &&
        config['isreplicaset'] != true
      end

      # Returns whether this server is an unknown, per the SDAM spec.
      #
      # @example Is the server description unknown?
      #   description.unknown?
      #
      # @return [ true, false ] If the server description is unknown.
      #
      # @since 2.0.0
      def unknown?
        config.empty? || !ok?
      end

      # @api private
      def ok?
        config[Operation::Result::OK] &&
          config[Operation::Result::OK] == 1 || false
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
      # @deprecated
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
      # @deprecated
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
        ok? && !(standalone? || mongos?)
      end

      # Whether this description is from a data-bearing server
      # (standalone, mongos, primary or secondary).
      #
      # @return [ true, false ] Whether the description is from a data-bearing
      #   server.
      #
      # @since 2.7.0
      def data_bearing?
        mongos? || primary? || secondary? || standalone?
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
        !!(address.to_s.downcase != me.downcase if me)
      end

      # opTime in lastWrite subdocument of the ismaster response.
      #
      # @return [ BSON::Timestamp ] The timestamp.
      #
      # @since 2.7.0
      def op_time
        if config['lastWrite'] && config['lastWrite']['opTime']
          config['lastWrite']['opTime']['ts']
        end
      end

      # Time when this server description was created.
      #
      # @note This time does not indicate when a successful server check
      # completed, because marking a server unknown updates its description
      # and last_update_time. Use Server#last_scan to find out when the server
      # was last successfully checked by its Monitor.
      #
      # @return [ Time ] Server description creation time.
      #
      # @since 2.7.0
      attr_reader :last_update_time

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

        (config.keys + other.config.keys).uniq.all? do |k|
          config[k] == other.config[k] || EXCLUDE_FOR_COMPARISON.include?(k)
        end
      end
      alias_method :eql?, :==

      # @api private
      def server_version_gte?(version)
        required_wv = case version
          when '4.2'
            8
          when '4.0'
            7
          when '3.6'
            6
          when '3.4'
            5
          when '3.2'
            4
          when '3.0'
            3
          when '2.6'
            2
          else
            raise ArgumentError, "Bogus required version #{version}"
          end

        required_wv >= min_wire_version && required_wv <= max_wire_version
      end
    end
  end
end
