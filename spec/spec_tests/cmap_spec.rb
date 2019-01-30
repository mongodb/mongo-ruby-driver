require 'lite_spec_helper'

describe 'CMAP' do

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:options).and_return({})
    end
  end

  CMAP_TESTS.sort.each do |file|
    spec = Mongo::CMAP::Spec.new(file)

    context("#{spec.description} (#{file.sub(%r'.*/data/cmap/', '')})") do
      let!(:result) do
        spec.run(cluster)
      end

      let(:verifier) do
        Mongo::CMAP::Verifier.new(spec)
      end

      it 'raises the correct error' do
        expect(result['error']).to eq(spec.error)
      end

      it 'emits the correct events' do
        verifier.verify_events(result['events'])
      end
    end
  end
end
