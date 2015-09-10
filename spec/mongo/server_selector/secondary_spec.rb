require 'spec_helper'

describe Mongo::ServerSelector::Secondary do

  let(:name) { :secondary }

  include_context 'server selector'

  it_behaves_like 'a server selector mode' do
    let(:slave_ok) { true }
  end
  it_behaves_like 'a server selector with sensitive data in its options'

  it_behaves_like 'a server selector accepting tag sets'

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
      let(:matching_secondary) { server(:secondary, :tags => server_tags) }

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
      let(:far_primary) { server(:primary, :average_round_trip_time => 100) }
      let(:far_secondary) { server(:secondary, :average_round_trip_time => 113) }

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
