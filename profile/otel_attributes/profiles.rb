# frozen_string_literal: true

module Mongo
  # Harnesses that measure the cost of OpenTelemetry span attributes: what the
  # SDK charges for carrying one, and what the driver charges for building one.
  module OtelAttributes
    # The attributes the driver can put on a span, with one realistic sample
    # value each.
    #
    # The values are literals on purpose. This harness measures the
    # OpenTelemetry SDK's cost of carrying an attribute, so building the value
    # must not be part of the measurement. The driver's cost of building the
    # value is measured separately, by ConstructionCost.
    SAMPLE = {
      'db.system.name' => 'mongodb',
      'db.namespace' => 'perftest',
      'db.command.name' => 'find',
      'db.collection.name' => 'corpus',
      'server.port' => 27_017,
      'server.address' => 'localhost',
      'network.transport' => 'tcp',
      'db.mongodb.server_connection_id' => 42,
      'db.mongodb.driver_connection_id' => 7,
      'db.query.summary' => 'find perftest.corpus',
      'db.mongodb.lsid' => '6f1d0f2e-0f9a-4f3b-9a3e-2f2b1c4d5e6f',
      'db.mongodb.cursor_id' => 123_456_789,
      'db.mongodb.txn_number' => 3
    }.freeze

    # The attributes the driver passes to start_span today: the cheap,
    # sampler-plausible set. The connection attributes are memoized per
    # connection in the driver; at the SDK boundary they are just five more
    # entries in the hash.
    CREATION_KEYS = %w[
      db.system.name db.namespace db.command.name db.collection.name
      server.port server.address network.transport
      db.mongodb.server_connection_id db.mongodb.driver_connection_id
    ].freeze

    # The attributes the driver sets after span creation, only when the span is
    # recording.
    DEFERRED_KEYS = %w[
      db.query.summary db.mongodb.lsid db.mongodb.cursor_id db.mongodb.txn_number
    ].freeze

    # One span shape: which attributes go to start_span, which are set
    # afterwards, and whether the attributes argument is passed at all.
    Profile = Struct.new(:name, :creation_keys, :deferred_keys, :bare) do
      # @return [ Hash | nil ] the attributes argument, or nil to omit it.
      def creation_attributes
        return nil if bare

        creation_keys.to_h { |key| [ key, SAMPLE[key] ] }
      end

      # @return [ Array<Array> ] key/value pairs to set after creation.
      def deferred_pairs
        deferred_keys.map { |key| [ key, SAMPLE[key] ] }
      end
    end

    # Every profile the sweep measures. Order is the order they are reported
    # in; the sweep rotates it each repetition.
    #
    # @return [ Array<Profile> ] the profiles.
    def self.profiles
      list = [
        Profile.new('none', [], [], true),
        Profile.new('empty-hash', [], [], false)
      ]
      SAMPLE.each_key { |key| list << Profile.new("one:#{key}", [ key ], [], false) }
      list + [
        Profile.new('creation-current', CREATION_KEYS, [], false),
        Profile.new('deferred-current', [], DEFERRED_KEYS, false),
        Profile.new('current-full', CREATION_KEYS, DEFERRED_KEYS, false),
        Profile.new('all-at-creation', SAMPLE.keys, [], false),
        Profile.new('none-then-all', [], SAMPLE.keys, false)
      ]
    end
  end
end
