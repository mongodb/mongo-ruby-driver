require 'spec_helper'

describe 'Server Selection moving average round trip time calculation' do

  include Mongo::ServerSelection::RTT

  SERVER_SELECTION_RTT_TESTS.each do |file|

    spec = Mongo::ServerSelection::RTT::Spec.new(file)

    context(spec.description) do

      let(:address) do
        Mongo::Address.new('127.0.0.1:27017')
      end

      let(:monitor) do
        Mongo::Server::Monitor.new(address, Mongo::Event::Listeners.new,
          Mongo::Monitoring.new,
          SpecConfig.instance.test_options)
      end

      before do
        monitor.instance_variable_set(:@average_round_trip_time, spec.average_rtt)
        expect(monitor).to receive(:round_trip_time).and_return(spec.new_rtt)
        monitor.scan!
      end

      it 'correctly calculates the moving average round trip time' do
        expect(monitor.description.average_round_trip_time).to eq(spec.new_average_rtt)
      end
    end
  end
end
