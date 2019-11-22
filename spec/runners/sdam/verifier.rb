module Sdam
  class Verifier
    include RSpec::Matchers

    def verify_sdam_event(expected_events, actual_events, i)
      expect(expected_events.length).to be > i
      expect(actual_events.length).to be > i

      expected_event = expected_events[i]
      actual_event = actual_events[i]

      actual_event_name = Utils.underscore(actual_event.class.name.sub(/.*::/, ''))
      actual_event_name = actual_event_name.to_s.sub('topology_changed', 'topology_description_changed') + '_event'
      expect(actual_event_name).to eq(expected_event.name)

      send("verify_#{expected_event.name}", expected_event, actual_event)
    end

    def verify_topology_opening_event(expected, actual)
      expect(actual.topology).not_to be nil
    end

    def verify_topology_description_changed_event(expected, actual)
      verify_topology_matches(expected.data['previousDescription'], actual.previous_topology)
      verify_topology_matches(expected.data['newDescription'], actual.new_topology)
    end

    def verify_topology_matches(expected, actual)
      expected_type = ::Mongo::Cluster::Topology.const_get(expected['topologyType'])
      expect(actual).to be_a(expected_type)

      expect(actual.replica_set_name).to eq(expected['setName'])

      expected['servers'].each do |server|
        desc = actual.server_descriptions[server['address'].to_s]
        expect(desc).not_to be nil
        verify_description_matches(server, desc)
      end

      # Verify actual topology has no servers not also present in the
      # expected topology description.
      expected_addresses = expected['servers'].map do |server|
        server['address']
      end
      actual.server_descriptions.keys.each do |address_str|
        expect(expected_addresses).to include(address_str)
      end
    end

    def verify_server_opening_event(expected, actual)
      expect(actual.address.to_s).to eq(expected.data['address'])
    end

    def verify_server_description_changed_event(expected, actual)
      verify_description_matches(expected.data['previousDescription'], actual.previous_description)
      verify_description_matches(expected.data['newDescription'], actual.new_description)
    end

    def verify_description_matches(expected, actual)
      case expected['type']
      when 'Standalone'
        expect(actual).to be_standalone
      when 'RSPrimary'
        expect(actual).to be_primary
      when 'RSSecondary'
        expect(actual).to be_secondary
      when 'RSArbiter'
        expect(actual).to be_arbiter
      when 'Mongos'
        expect(actual).to be_mongos
      when 'Unknown', 'PossiblePrimary'
        expect(actual).to be_unknown
      when 'RSGhost'
        expect(actual).to be_ghost
      when 'RSOther'
        expect(actual).to be_other
      end

      expect(actual.address.to_s).to eq(expected['address'])
      expect(actual.arbiters).to eq(expected['arbiters'])
      expect(actual.hosts).to eq(expected['hosts'])
      expect(actual.passives).to eq(expected['passives'])
      expect(actual.primary_host).to eq(expected['primary'])
      expect(actual.replica_set_name).to eq(expected['setName'])
    end

    def verify_server_closed_event(expected, actual)
      expect(actual.address.to_s).to eq(expected.data['address'])
    end
  end
end
