require 'lite_spec_helper'

describe 'Cmap' do

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:options).and_return({})
    end
  end

  CMAP_TESTS.each do |file|
    spec = Mongo::Cmap::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/cmap/', '')})") do
      before do
        spec.setup(cluster)
      end

      let!(:result) do
        mock_socket = double('socket')
        allow(mock_socket).to receive(:close)
        allow_any_instance_of(Mongo::Server::Connection).to receive(:do_connect).and_return(mock_socket)
        spec.run
      end

      let(:verifier) do
        Mongo::Cmap::Verifier.new(spec)
      end

      it 'raises the correct error' do
        expect(result['error']).to eq(spec.expected_error)
      end

      let(:actual_events) { result['events'].freeze }

      it 'emits the correct number of events' do
        expect(actual_events.length).to eq(spec.expected_events.length)
      end

      spec.expected_events.each_with_index do |expected_event, index|
        it "emits correct event #{index+1}" do
          verifier.verify_hashes(actual_events[index], expected_event)
        end
      end
    end
  end
end
