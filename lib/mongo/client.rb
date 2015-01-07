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

require 'mongo/write_concern/mode'
require 'mongo/write_concern/acknowledged'
require 'mongo/write_concern/unacknowledged'

module Mongo

  # The client is the entry point to the driver and is the main object that
  # will be interacted with.
  #
  # @since 2.0.0
  class Client
    extend Forwardable

    # @return [ Mongo::Cluster ] cluster The cluster of servers for the client.
    attr_reader :cluster

    # @return [ Mongo::Database ] database The database the client is operating on.
    attr_reader :database

    # @return [ Event::Listeners ] event_listeners The event listeners for the
    #   client.
    attr_reader :event_listeners

    # @return [ Hash ] options The configuration options.
    attr_reader :options

    # Delegate command execution to the current database.
    def_delegators :@database, :command

    # Determine if this client is equivalent to another object.
    #
    # @example Check client equality.
    #   client == other
    #
    # @param [ Object ] other The object to compare to.
    #
    # @return [ true, false ] If the objects are equal.
    #
    # @since 2.0.0
    def ==(other)
      return false unless other.is_a?(Client)
      cluster == other.cluster && options == other.options
    end
    alias_method :eql?, :==

    # Get a collection object for the provided collection name.
    #
    # @example Get the collection.
    #   client[:users]
    #
    # @param [ String, Symbol ] collection_name The name of the collection.
    # @param [ Hash ] options The options to the collection.
    #
    # @return [ Mongo::Collection ] The collection.
    #
    # @since 2.0.0
    def [](collection_name, options = {})
      database[collection_name, options]
    end

    # Get the hash value of the client.
    #
    # @example Get the client hash value.
    #   client.hash
    #
    # @return [ Integer ] The client hash value.
    #
    # @since 2.0.0
    def hash
      [cluster, options].hash
    end

    # Instantiate a new driver client.
    #
    # @example Instantiate a single server or mongos client.
    #   Mongo::Client.new([ '127.0.0.1:27017' ])
    #
    # @example Instantiate a client for a replica set.
    #   Mongo::Client.new([ '127.0.0.1:27017', '127.0.0.1:27021' ])
    #
    # @param [ Array<String>, String ] addresses_or_uri The array of server addresses in the
    #   form of host:port or a MongoDB URI connection string.
    # @param [ Hash ] options The options to be used by the client.
    #
    # @option options [ Symbol ] :auth_mech
    # @option options [ String ] :auth_source
    # @option options [ String ] :database
    # @option options [ Hash ] :auth_mech_properties
    # @option options [ Float ] :heartbeat_frequency
    # @option options [ Symbol ] :mode
    # @option options [ String ] :password
    # @option options [ Integer ] :max_pool_size
    # @option options [ Integer ] :min_pool_size
    # @option options [ Float ] :wait_queue_timeout
    # @option options [ Float ] :connect_timeout
    # @option options [ Hash ] :read
    # @option options [ Array<Hash, String> ] :roles
    # @option options [ Symbol ] :replica_set_name
    # @option options [ true, false ] :ssl
    # @option options [ Float ] :socket_timeout
    # @option options [ String ] :user
    # @option options [ Symbol ] :write
    #
    # @since 2.0.0
    def initialize(addresses_or_uri, options = {})
      @event_listeners = Event::Listeners.new
      if addresses_or_uri.is_a?(::String)
        create_from_uri(addresses_or_uri, options)
      else
        create_from_addresses(addresses_or_uri, options)
      end
    end

    # Get an inspection of the client as a string.
    #
    # @example Inspect the client.
    #   client.inspect
    #
    # @return [ String ] The inspection string.
    #
    # @since 2.0.0
    def inspect
      "<Mongo::Client:0x#{object_id} cluster=#{cluster.addresses.join(', ')}>"
    end

    # Get the server (read) preference from the options passed to the client.
    #
    # @example Get the server (read) preference.
    #   client.server_preference
    #
    # @return [ Object ] The appropriate server preference or primary if none
    #   was provided to the client.
    #
    # @since 2.0.0
    def server_preference
      @server_preference ||= ServerPreference.get(options[:read] || {})
    end

    # Use the database with the provided name. This will switch the current
    # database the client is operating on.
    #
    # @example Use the provided database.
    #   client.use(:users)
    #
    # @param [ String, Symbol ] name The name of the database to use.
    #
    # @return [ Mongo::Client ] The new client with new database.
    #
    # @since 2.0.0
    def use(name)
      with(database: name)
    end

    # Provides a new client with the passed options merged over the existing
    # options of this client. Useful for one-offs to change specific options
    # without altering the original client.
    #
    # @example Get a client with changed options.
    #   client.with(:read => { :mode => :primary_preferred })
    #
    # @param [ Hash ] new_options The new options to use.
    #
    # @return [ Mongo::Client ] A new client instance.
    #
    # @since 2.0.0
    def with(new_options = {})
      Client.new(cluster.addresses.dup, options.merge(new_options))
    end

    # Get the write concern for this client. If no option was provided, then a
    # default single server acknowledgement will be used.
    #
    # @example Get the client write concern.
    #   client.write_concern
    #
    # @return [ Mongo::WriteConcern::Mode ] The write concern.
    #
    # @since 2.0.0
    def write_concern
      @write_concern ||= WriteConcern::Mode.get(options[:write])
    end

    private

    def create_from_addresses(addresses, options = {})
      @options = create_options(options)
      @cluster = Cluster.new(addresses, server_preference, event_listeners, @options)
      @database = Database.new(self, @options[:database])
    end

    def create_from_uri(connection_string, options = {})
      uri = URI.new(connection_string)
      @options = create_options(uri.client_options.merge(options))
      @cluster = Cluster.new(uri.servers, server_preference, event_listeners, @options)
      @database = Database.new(self, @options[:database])
    end

    def create_options(options = {})
      { :database => Database::ADMIN }.merge(options).freeze
    end
  end
end
