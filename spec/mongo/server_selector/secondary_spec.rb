require 'spec_helper'

describe Mongo::ServerSelector::Secondary do

  let(:name) { :secondary }

  include_context 'server selector'

  it_behaves_like 'a server selector mode' do
    let(:slave_ok) { true }
  end
  it_behaves_like 'a server selector with sensitive data in its options'

  it_behaves_like 'a server selector accepting tag sets'

  describe '#initialize' do

    context 'when max_staleness is provided' do

      let(:options) do
        { max_staleness: 100 }
      end

      it 'sets the max_staleness option' do
        expect(selector.max_staleness).to eq(options[:max_staleness])
      end
    end
  end

  describe '#==' do

    context 'when max staleness is the same' do

      let(:options) do
        { max_staleness: 90 }
      end

      let(:other) do
        described_class.new(options)
      end

      it 'returns true' do
        expect(selector).to eq(other)
      end
    end

    context 'when max staleness is different' do

      let(:other_options) do
        { max_staleness: 95 }
      end

      let(:other) do
        described_class.new(other_options)
      end

      it 'returns false' do
        expect(selector).not_to eq(other)
      end
    end
  end

  describe '#to_mongos' do

    it 'returns read preference formatted for mongos' do
      expect(selector.to_mongos).to eq(
        { :mode => 'secondary' }
      )
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }

      it 'returns read preference formatted for mongos with tag sets' do
        expect(selector.to_mongos).to eq(
          { :mode => 'secondary', :tags => tag_sets}
        )
      end
    end

    context 'max staleness not provided' do

      let(:expected) do
        { :mode => 'secondary' }
      end

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq(expected)
      end
    end

    context 'max staleness provided' do

      let(:max_staleness) do
        60
      end

      let(:expected) do
        { :mode => 'secondary', maxStalenessSeconds: 60 }
      end

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq(expected)
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

    context 'single primary candidate' do
      let(:candidates) { [primary] }

      it 'returns an empty array' do
        expect(selector.send(:select, candidates)).to be_empty
      end
    end

    context 'single secondary candidate' do
      let(:candidates) { [secondary] }

      it 'returns array with secondary' do
        expect(selector.send(:select, candidates)).to eq([secondary])
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [primary, secondary] }

      it 'returns array with secondary' do
        expect(selector.send(:select, candidates)).to eq([secondary])
      end
    end

    context 'multiple secondary candidates' do
      let(:candidates) { [secondary, secondary, primary] }

      it 'returns array with all secondaries' do
        expect(selector.send(:select, candidates)).to eq([secondary, secondary])
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      let(:matching_secondary) { make_server(:secondary, :tags => server_tags, address: default_address) }

      context 'single candidate' do

        context 'primary' do
          let(:candidates) { [primary] }

          it 'returns an empty array' do
            expect(selector.send(:select, candidates)).to be_empty
          end
        end

        context 'secondary' do
          let(:candidates) { [secondary] }

          it 'returns an empty array' do
            expect(selector.send(:select, candidates)).to be_empty
          end
        end

        context 'matching secondary' do
          let(:candidates) { [matching_secondary] }

          it 'returns an array with matching secondary' do
            expect(selector.send(:select, candidates)).to eq([matching_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'no matching candidates' do
          let(:candidates) { [primary, secondary, secondary] }

          it 'returns an emtpy array' do
            expect(selector.send(:select, candidates)).to be_empty
          end
        end

        context 'one matching secondary' do
          let(:candidates) { [secondary, matching_secondary]}

          it 'returns array with matching secondary' do
            expect(selector.send(:select, candidates)).to eq([matching_secondary])
          end
        end

        context 'two matching secondaries' do
          let(:candidates) { [matching_secondary, matching_secondary] }

          it 'returns an array with both matching secondaries' do
            expect(selector.send(:select, candidates)).to eq([matching_secondary, matching_secondary])
          end
        end
      end
    end

    context 'high latency servers' do
      let(:far_primary) { make_server(:primary, :average_round_trip_time => 0.100, address: default_address) }
      let(:far_secondary) { make_server(:secondary, :average_round_trip_time => 0.113, address: default_address) }

      context 'single candidate' do

        context 'far primary' do
          let(:candidates) { [far_primary] }

          it 'returns an empty array' do
            expect(selector.send(:select, candidates)).to be_empty
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns an array with the secondary' do
            expect(selector.send(:select, candidates)).to eq([far_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }

          it 'returns an array with the secondary' do
            expect(selector.send(:select, candidates)).to eq([far_secondary])
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }

          it 'returns an array with the secondary' do
            expect(selector.send(:select, candidates)).to eq([far_secondary])
          end
        end

        context 'two near servers, one far server' do

          context 'near primary, near and far secondaries' do
            let(:candidates) { [primary, secondary, far_secondary] }

            it 'returns an array with near secondary' do
              expect(selector.send(:select, candidates)).to eq([secondary])
            end
          end

          context 'far primary and two near secondaries' do
            let(:candidates) { [far_primary, secondary, secondary] }

            it 'returns an array with two secondaries' do
              expect(selector.send(:select, candidates)).to eq([secondary, secondary])
            end
          end
        end
      end
    end
  end
end
