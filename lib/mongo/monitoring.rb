# Copyright (C) 2015-2019 MongoDB, Inc.
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

  # The class defines behavior for the performance monitoring API.
  #
  # @since 2.1.0
  class Monitoring
    include Id

    # The command topic.
    #
    # @since 2.1.0
    COMMAND = 'Command'.freeze

    # The connection pool topic.
    #
    # @since 2.8.0
    CONNECTION_POOL = 'ConnectionPool'.freeze

    # Server closed topic.
    #
    # @since 2.4.0
    SERVER_CLOSED = 'ServerClosed'.freeze

    # Server description changed topic.
    #
    # @since 2.4.0
    SERVER_DESCRIPTION_CHANGED = 'ServerDescriptionChanged'.freeze

    # Server opening topic.
    #
    # @since 2.4.0
    SERVER_OPENING = 'ServerOpening'.freeze

    # Topology changed topic.
    #
    # @since 2.4.0
    TOPOLOGY_CHANGED = 'TopologyChanged'.freeze

    # Topology closed topic.
    #
    # @since 2.4.0
    TOPOLOGY_CLOSED = 'TopologyClosed'.freeze

    # Topology opening topic.
    #
    # @since 2.4.0
    TOPOLOGY_OPENING = 'TopologyOpening'.freeze

    # Server heartbeat started topic.
    #
    # @since 2.7.0
    SERVER_HEARTBEAT = 'ServerHeartbeat'.freeze

    # Used for generating unique operation ids to link events together.
    #
    # @example Get the next operation id.
    #   Monitoring.next_operation_id
    #
    # @return [ Integer ] The next operation id.
    #
    # @since 2.1.0
    def self.next_operation_id
      self.next_id
    end

    # Contains subscription methods common between monitoring and
    # global event subscriptions.
    #
    # @since 2.6.0
    module Subscribable
      # Subscribe a listener to an event topic.
      #
      # @note It is possible to subscribe the same listener to the same topic
      # multiple times, in which case the listener will be invoked as many
      # times as it is subscribed and to unsubscribe it the same number
      # of unsubscribe calls will be needed.
      #
      # @example Subscribe to the topic.
      #   monitoring.subscribe(QUERY, subscriber)
      #
      # @example Subscribe to the topic globally.
      #   Monitoring::Global.subscribe(QUERY, subscriber)
      #
      # @param [ String ] topic The event topic.
      # @param [ Object ] subscriber The subscriber to handle the event.
      #
      # @since 2.1.0
      def subscribe(topic, subscriber)
        subscribers_for(topic).push(subscriber)
      end

      # Unsubscribe a listener from an event topic.
      #
      # If the listener was subscribed to the event topic multiple times,
      # this call removes a single subscription.
      #
      # If the listener was not subscribed to the topic, this operation
      # is a no-op and no exceptions are raised.
      #
      # @note Global subscriber registry is separate from per-client
      #   subscriber registry. The same subscriber can be subscribed to
      #   events from a particular client as well as globally; unsubscribing
      #   globally will not unsubscribe that subscriber from the client
      #   it was explicitly subscribed to.
      #
      # @note Currently the list of global subscribers is copied into
      #   a client whenever the client is created. Thus unsubscribing a
      #   subscriber globally has no effect for existing clients - they will
      #   continue sending events to the unsubscribed subscriber.
      #
      # @example Unsubscribe from the topic.
      #   monitoring.unsubscribe(QUERY, subscriber)
      #
      # @example Unsubscribe from the topic globally.
      #   Mongo::Monitoring::Global.unsubscribe(QUERY, subscriber)
      #
      # @param [ String ] topic The event topic.
      # @param [ Object ] subscriber The subscriber to be unsubscribed.
      #
      # @since 2.6.0
      def unsubscribe(topic, subscriber)
        subs = subscribers_for(topic)
        index = subs.index(subscriber)
        if index
          subs.delete_at(index)
        end
      end

      # Get all the subscribers.
      #
      # @example Get all the subscribers.
      #   monitoring.subscribers
      #
      # @example Get all the global subscribers.
      #   Mongo::Monitoring::Global.subscribers
      #
      # @return [ Hash<String, Object> ] The subscribers.
      #
      # @since 2.1.0
      def subscribers
        @subscribers ||= {}
      end

      # Determine if there are any subscribers for a particular event.
      #
      # @example Are there subscribers?
      #   monitoring.subscribers?(COMMAND)
      #
      # @example Are there global subscribers?
      #   Mongo::Monitoring::Global.subscribers?(COMMAND)
      #
      # @param [ String ] topic The event topic.
      #
      # @return [ true, false ] If there are subscribers for the topic.
      #
      # @since 2.1.0
      def subscribers?(topic)
        !subscribers_for(topic).empty?
      end

      private

      def subscribers_for(topic)
        subscribers[topic] ||= []
      end
    end

    # Allows subscribing to events for all Mongo clients.
    #
    # @note Global subscriptions must be established prior to creating
    #   clients. When a client is constructed it copies subscribers from
    #   the Global module; subsequent subscriptions or unsubscriptions
    #   on the Global module have no effect on already created clients.
    #
    # @since 2.1.0
    module Global
      extend Subscribable
    end

    include Subscribable

    # Initialize the monitoring.
    #
    # @example Create the new monitoring.
    #   Monitoring.new(:monitoring => true)
    #
    # @param [ Hash ] options Options. Client constructor forwards its
    #   options to Monitoring constructor, although Monitoring recognizes
    #   only a subset of the options recognized by Client.
    # @option options [ true, false ] :monitoring If false is given, the
    #   Monitoring instance is initialized without global monitoring event
    #   subscribers and will not publish SDAM events. Command monitoring events
    #   will still be published, and the driver will still perform SDAM and
    #   monitor its cluster in order to perform server selection. Built-in
    #   driver logging of SDAM events will be disabled because it is
    #   implemented through SDAM event subscription. Client#subscribe will
    #   succeed for all event types, but subscribers to SDAM events will
    #   not be invoked. Values other than false result in default behavior
    #   which is to perform normal SDAM event publication.
    #
    # @since 2.1.0
    # @api private
    def initialize(options = {})
      @options = options
      if options[:monitoring] != false
        Global.subscribers.each do |topic, subscribers|
          subscribers.each do |subscriber|
            subscribe(topic, subscriber)
          end
        end
        subscribe(COMMAND, CommandLogSubscriber.new(options))
        subscribe(CONNECTION_POOL, CmapLogSubscriber.new(options))
        subscribe(SERVER_OPENING, ServerOpeningLogSubscriber.new(options))
        subscribe(SERVER_CLOSED, ServerClosedLogSubscriber.new(options))
        subscribe(SERVER_DESCRIPTION_CHANGED, ServerDescriptionChangedLogSubscriber.new(options))
        subscribe(TOPOLOGY_OPENING, TopologyOpeningLogSubscriber.new(options))
        subscribe(TOPOLOGY_CHANGED, TopologyChangedLogSubscriber.new(options))
        subscribe(TOPOLOGY_CLOSED, TopologyClosedLogSubscriber.new(options))
      end
    end

    # @api private
    attr_reader :options

    # @api private
    def monitoring?
      options[:monitoring] != false
    end

    # Publish a started event.
    #
    # @example Publish a started event.
    #   monitoring.started(COMMAND, event)
    #
    # @param [ String ] topic The event topic.
    # @param [ Event ] event The event to publish.
    #
    # @since 2.1.0
    def started(topic, event)
      subscribers_for(topic).each{ |subscriber| subscriber.started(event) }
    end

    # Publish a succeeded event.
    #
    # @example Publish a succeeded event.
    #   monitoring.succeeded(COMMAND, event)
    #
    # @param [ String ] topic The event topic.
    # @param [ Event ] event The event to publish.
    #
    # @since 2.1.0
    def succeeded(topic, event)
      subscribers_for(topic).each{ |subscriber| subscriber.succeeded(event) }
    end

    # Publish a failed event.
    #
    # @example Publish a failed event.
    #   monitoring.failed(COMMAND, event)
    #
    # @param [ String ] topic The event topic.
    # @param [ Event ] event The event to publish.
    #
    # @since 2.1.0
    def failed(topic, event)
      subscribers_for(topic).each{ |subscriber| subscriber.failed(event) }
    end

    private

    def initialize_copy(original)
      @subscribers = {}
      original.subscribers.each do |k, v|
        @subscribers[k] = v.dup
      end
    end
  end
end

require 'mongo/monitoring/event'
require 'mongo/monitoring/publishable'
require 'mongo/monitoring/command_log_subscriber'
require 'mongo/monitoring/cmap_log_subscriber'
require 'mongo/monitoring/sdam_log_subscriber'
require 'mongo/monitoring/server_description_changed_log_subscriber'
require 'mongo/monitoring/server_closed_log_subscriber'
require 'mongo/monitoring/server_opening_log_subscriber'
require 'mongo/monitoring/topology_changed_log_subscriber'
require 'mongo/monitoring/topology_opening_log_subscriber'
require 'mongo/monitoring/topology_closed_log_subscriber'
