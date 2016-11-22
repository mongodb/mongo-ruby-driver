require 'spec_helper'

describe Mongo::ServerSelector::Primary do

  let(:name) { :primary }

  include_context 'server selector'

  it_behaves_like 'a server selector mode' do
    let(:slave_ok) { false }
  end
  it_behaves_like 'a server selector with sensitive data in its options'

  describe '#initialize' do

    context 'when max_staleness is provided' do

      let(:options) do
        { max_staleness: 100 }
      end

      it 'raises an exception' do
        expect {
          selector
        }.to raise_exception(Mongo::Error::InvalidServerPreference)
      end
    end
  end

  describe '#tag_sets' do

    context 'tags not provided' do

      it 'returns an empty array' do
        expect(selector.tag_sets).to be_empty
      end
    end

    context 'tag sets provided' do

      let(:tag_sets) do
        [ tag_set ]
      end

      it 'raises an error' do
        expect {
          selector.tag_sets
        }.to raise_error(Mongo::Error::InvalidServerPreference)
      end
    end
  end

  describe '#to_mongos' do

    it 'returns nil' do
      expect(selector.to_mongos).to be_nil
    end

    context 'max staleness not provided' do

      it 'returns nil' do
        expect(selector.to_mongos).to be_nil
      end
    end

    context 'max staleness provided' do

      let(:max_staleness) do
        100
      end

      it 'raises an error' do
        expect {
          selector
        }.to raise_exception(Mongo::Error::InvalidServerPreference)
      end
    end
  end

  describe '#select' do

    context 'no candidates' do
      let(:candidates) { [] }

      it 'returns an empty array' do
        expect(selector.send(:select, candidates)).to be_empty
      end
    end

    context 'secondary candidates' do
      let(:candidates) { [secondary] }

      it 'returns an empty array' do
        expect(selector.send(:select, candidates)).to be_empty
      end
    end

    context 'primary candidate' do
      let(:candidates) { [primary] }

      it 'returns an array with the primary' do
        expect(selector.send(:select, candidates)).to eq([primary])
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [secondary, primary] }

      it 'returns an array with the primary' do
        expect(selector.send(:select, candidates)).to eq([primary])
      end
    end

    context 'high latency candidates' do
      let(:far_primary) { make_server(:primary, :average_round_trip_time => 0.100, address: default_address) }
      let(:far_secondary) { make_server(:secondary, :average_round_trip_time => 0.120, address: default_address) }

      context 'single candidate' do

        context 'far primary' do
          let(:candidates) { [far_primary] }

          it 'returns array with the primary' do
            expect(selector.send(:select, candidates)).to eq([far_primary])
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns empty array' do
            expect(selector.send(:select, candidates)).to be_empty
          end
        end
      end

      context 'multiple candidates' do

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }

          it 'returns an array with the primary' do
            expect(selector.send(:select, candidates)).to eq([far_primary])
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, far_secondary] }

          it 'returns an array with the primary' do
            expect(selector.send(:select, candidates)).to eq([far_primary])
          end
        end
      end
    end
  end
end
