require 'lite_spec_helper'

describe Mongo::ClusterTime do
  describe '#>=' do
    context 'equal but different objects' do
      let(:one) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }
      let(:two) { described_class.new(clusterTime: BSON::Timestamp.new(123, 456)) }

      it 'is true' do
        expect(one).to be >= two
      end
    end
  end
end
