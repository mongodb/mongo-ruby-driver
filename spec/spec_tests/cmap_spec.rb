require 'lite_spec_helper'

describe 'Cmap' do

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:options).and_return({})
    end
  end

  CMAP_TESTS.sort.each do |file|
    spec = Mongo::Cmap::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/cmap/', '')})") do
      before do
        spec.setup(cluster)
      end

      let!(:result) do
        spec.run
      end

      let(:verifier) do
        Mongo::Cmap::Verifier.new(spec)
      end

      it 'raises the correct error' do
        expect(result['error']).to eq(spec.expected_error)
      end

      it 'emits the correct events' do
        verifier.verify_events(result['events'])
      end
    end
  end
end
