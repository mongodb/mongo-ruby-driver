require 'spec_helper'

describe Mongo::NodePreference::SecondaryPreferred do
  include_context 'read preference'

  it_behaves_like 'a read preference mode' do
    let(:name) { :secondary_preferred }
    let(:slave_ok) { true }
  end

  it_behaves_like 'a read preference mode accepting tag sets'

  describe '#to_mongos' do

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }

      it 'returns a read preference formatted for mongos' do
        expect(read_pref.to_mongos).to eq(
          { :mode => 'secondaryPreferred', :tags => tag_sets}
        )
      end
    end

    context 'tag sets not provided' do

      it 'returns nil' do
        expect(read_pref.to_mongos).to be_nil
      end
    end
  end

  describe '#select_nodes' do

    context 'no candidates' do
      let(:candidates) { [] }

      it 'returns an empty array' do
        expect(read_pref.select_nodes(candidates)).to be_empty
      end
    end

    context 'single primary candidates' do
      let(:candidates) { [primary] }

      it 'returns array with primary' do
        expect(read_pref.select_nodes(candidates)).to eq([primary])
      end
    end

    context 'single secondary candidate' do
      let(:candidates) { [secondary] }

      it 'returns array with secondary' do
        expect(read_pref.select_nodes(candidates)).to eq([secondary])
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [primary, secondary] }
      let(:expected) { [secondary, primary] }

      it 'returns array with secondary first, then primary' do
        expect(read_pref.select_nodes(candidates)).to eq(expected)
      end
    end

    context 'secondary and primary candidates' do
      let(:candidates) { [secondary, primary] }
      let(:expected) { [secondary, primary] }

      it 'returns array with secondary first, then primary' do
        expect(read_pref.select_nodes(candidates)).to eq(expected)
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      let(:matching_primary) do
        node(:primary, :tags => tag_sets)
      end
      let(:matching_secondary) do
        node(:secondary, :tags => tag_sets)
      end

      context 'single candidate' do

        context 'primary' do
          let(:candidates) { [primary] }

          it 'returns array with primary' do
            expect(read_pref.select_nodes(candidates)).to eq([primary])
          end
        end

        context 'matching_primary' do
          let(:candidates) { [matching_primary] }

          it 'returns array with matching primary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_primary])
          end
        end

        context 'matching secondary' do
          let(:candidates) { [matching_secondary] }

          it 'returns array with matching secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_secondary])
          end
        end

        context 'secondary' do
          let(:candidates) { [secondary] }

          it 'returns an empty array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
          end
        end
      end

      context 'multiple candidates' do

        context 'no matching secondaries' do
          let(:candidates) { [primary, secondary, secondary] }

          it 'returns an array with the primary' do
            expect(read_pref.select_nodes(candidates)).to eq([primary])
          end
        end

        context 'one matching secondary' do
          let(:candidates) { [primary, matching_secondary] }

          it 'returns an array of the matching secondary, then primary' do
            expect(read_pref.select_nodes(candidates)).to eq(
              [matching_secondary, primary]
            )
          end
        end

        context 'two matching secondaries' do
          let(:candidates) { [primary, matching_secondary, matching_secondary] }
          let(:expected) { [matching_secondary, matching_secondary, primary] }

          it 'returns an array of the matching secondary, then primary' do
            expect(read_pref.select_nodes(candidates)).to eq(expected)
          end
        end

        context 'one matching secondary and one matching primary' do
          let(:candidates) { [matching_primary, matching_secondary] }
          let(:expected) {[matching_secondary, matching_primary] }

          it 'returns an array of the matching secondary, then the primary' do
            expect(read_pref.select_nodes(candidates)).to eq(expected)
          end
        end
      end
    end

    context 'high latency nodes' do
      let(:far_primary) { node(:primary, :ping => 100) }
      let(:far_secondary) { node(:secondary, :ping => 113) }

      context 'single candidate' do

        context 'far primary' do
          let(:candidates) { [far_primary] }

          it 'returns array with primary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_primary])
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns an array with the secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'local primary, local secondary' do
          let(:candidates) { [primary, secondary] }

          it 'returns an array with secondary, then primary' do
            expect(read_pref.select_nodes(candidates)).to eq([secondary, primary])
          end
        end

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }

          it 'returns an array with the secondary, then primary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_secondary, primary])
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, secondary] }
          let(:expected) { [secondary, far_primary] }

          it 'returns an array with secondary, then primary' do
            expect(read_pref.select_nodes(candidates)).to eq(expected)
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }
          let(:expected) { [far_secondary, far_primary] }

          it 'returns an array with secondary, then primary' do
            expect(read_pref.select_nodes(candidates)).to eq(expected)
          end
        end

        context 'two near nodes, one far node' do

          context 'near primary, near secondary, far secondary' do
            let(:candidates) { [primary, secondary, far_secondary] }
            let(:expected) { [secondary, primary] }

            it 'returns an array with near secondary, then primary' do
              expect(read_pref.select_nodes(candidates)).to eq(expected)
            end
          end

          context 'two near secondaries, one far primary' do
            let(:candidates) { [far_primary, secondary, secondary] }
            let(:expected) { [secondary, secondary, far_primary] }

            it 'returns an array with secondaries, then primary' do
              expect(read_pref.select_nodes(candidates)).to eq(expected)
            end
          end
        end
      end
    end
  end
end