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

      it 'successfully runs the test' do
        result = spec.run(cluster)
        expect(result['error']).to eq(spec.error)
        expect(spec.events).to match_events(result['events'])
      end
    end
  end
end
