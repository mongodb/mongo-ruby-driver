# frozen_string_literal: true

require 'faas_test/subscribers/heartbeat'
require 'faas_test/subscribers/command'
require 'faas_test/subscribers/connection_pool'

module FaaSTest
  class Runner
    extend Forwardable

    attr_reader :client
    attr_reader :collection_name

    def_delegators :client, :database

    def initialize(client)
      @client = client
      @collection_name = BSON::ObjectId.new.to_s

      prepare_subscriptions!
    end

    def run
      perform_test
      compile_results
    end

    private

    attr_reader :heartbeat_subscriber
    attr_reader :command_subscriber
    attr_reader :connection_pool_subscriber

    def compile_results
      {
        heartbeat: {
          started: heartbeat_subscriber.started_count,
          succeeded: heartbeat_subscriber.succeeded_count,
          failed: heartbeat_subscriber.failed_count,
          durations: heartbeat_subscriber.durations,
        },
        command: {
          durations: command_subscriber.durations,
        },
        connections: {
          open: connection_pool_subscriber.open_connections,
        }
      }
    end

    def perform_test
      result = database[collection_name].insert_one({ a: 1, b: '2' })
      id = result.inserted_id

      database[collection_name].delete_one(_id: id)
    end

    def prepare_subscriptions!
      @heartbeat_subscriber = FaaSTest::Subscribers::Heartbeat.new
      @command_subscriber = FaaSTest::Subscribers::Command.new
      @connection_pool_subscriber = FaaSTest::Subscribers::ConnectionPool.new

      client.subscribe(Mongo::Monitoring::SERVER_HEARTBEAT, heartbeat_subscriber)
      client.subscribe(Mongo::Monitoring::COMMAND, command_subscriber)
      client.subscribe(Mongo::Monitoring::CONNECTION_POOL, connection_pool_subscriber)
    end
  end
end
