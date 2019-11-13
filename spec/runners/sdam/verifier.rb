module Sdam
  class Verifier
    include RSpec::Matchers
    include Mongo::SDAMMonitoring::Matchable

    def initialize(test_instance)
      @test_instance = test_instance
    end

    attr_reader :test_instance

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
      expect(topologies_match?(actual, expected)).to be true
    end

    def verify_server_opening_event(expected, actual)
      expect(actual.address.to_s).to eq(expected.data['address'])
    end

    def verify_server_description_changed_event(expected, actual)
      expect(descriptions_match?(actual, expected)).to be true
    end

    def verify_server_closed_event(expected, actual)
      expect(actual.address.to_s).to eq(expected.data['address'])
    end
  end
end
