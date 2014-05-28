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

module Mongo
  class Server

    # Represents a description of the server, populated by the result of the
    # ismaster command.
    #
    # @since 2.0.0
    class Description
      include Event::Publisher

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

      # Constant for reading max bson size info from config.
      #
      # @since 2.0.0
      MAX_BSON_OBJECT_SIZE = 'maxBsonObjectSize'.freeze

      # Constant for reading max message size info from config.
      #
      # @since 2.0.0
      MAX_MESSAGE_BYTES = 'maxMessageSizeBytes'.freeze

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

      # @return [ Hash ] The actual result from the isnamster command.
      attr_reader :config

      # Will return true if the server is an arbiter.
      #
      # @example Is the server an arbiter?
      #   description.arbiter?
      #
      # @return [ true, false ] If the server is an arbiter.
      #
      # @since 2.0.0
      def arbiter?
        !!config[ARBITER]
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
        config[HOSTS]
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
      def initialize(config = {})
        @config = config
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
        !!config[PRIMARY]
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
        !!config[SECONDARY]
      end

      # Get the name of the replica set the server belongs to, returns nil if
      # none.
      #
      # @example Get the replica set name.
      #   description.set_name
      #
      # @return [ String, nil ] The name of the replica set.
      #
      # @since 2.0.0
      def set_name
        config[SET_NAME]
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
      def update!(new_config)
        find_new_servers(new_config)
        find_removed_servers(new_config)
        @config = new_config
        self
      end

      private

      def find_new_servers(new_config)
        new_config[HOSTS].each do |host|
          publish(Event::HOST_ADDED, host) unless hosts.include?(host)
        end
      end

      def find_removed_servers(new_config)
        hosts.each do |host|
          publish(Event::HOST_REMOVED, host) unless new_config[HOSTS].include?(host)
        end
      end
    end
  end
end
