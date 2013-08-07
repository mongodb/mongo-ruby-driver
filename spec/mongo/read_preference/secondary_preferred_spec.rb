require 'spec_helper'

describe Mongo::ReadPreference::SecondaryPreferred do
  include_context 'read preference'

  it_behaves_like 'a read preference mode' do
    let(:name) { :secondary_preferred }
  end

  describe '#to_mongos' do
    it 'returns nil' do
      expect(pref.to_mongos).to be_nil
    end

    context 'tag sets provided' do
      let(:tag_sets) { [{ 'dc' => 'ny' }] }
      it 'returns mongos readPreference with tag sets' do
        expect(pref.to_mongos).to eq(
          { :mode => 'secondaryPreferred', :tags => tag_sets }
        )
      end
    end
  end

  describe '#select_nodes' do
    context 'no candidates' do
      let(:candidates) { [] }
      it 'returns empty array' do
        expect(pref.select_nodes(candidates)).to be_empty
      end
    end

    context 'primary candidate' do
      let(:candidates) { [primary] }
      it 'returns array with candidate' do
        expect(pref.select_nodes(candidates)).to eq(candidates)
      end
    end

    context 'secondary candidate' do
      let(:candidates) { [secondary] }
      it 'returns array with candidate' do
        expect(pref.select_nodes(candidates)).to eq(candidates)
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [primary, secondary] }
      let(:expected) { [secondary, primary] }
      it 'returns array with secondary followed by primary' do
        expect(pref.select_nodes(candidates)).to eq(expected)
      end
    end

    context 'secondary and primary candidates' do
      let(:candidates) { [secondary, primary] }
      it 'returns array with secondary followed by primary' do
        expect(pref.select_nodes(candidates)).to eq(candidates)
      end
    end

    context 'tag sets provided' do
      let(:ny_tags) { { 'dc' => 'ny' } }
      let(:tag_sets) { [ny_tags] }
      let(:matching_primary) do
        node(:primary, :tags => tag_set)
      end
      let(:matching_secondary) do
        node(:secondary, :tags => ny_tags)
      end

      context 'single candidate' do
        context 'primary' do
          let(:candidates) { [primary] }
          it 'returns array with primary' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end

        context 'matching primary' do
          let(:candidates) { [matching_primary] }
          it 'returns array with matching node' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end

        context 'secondary' do
          let(:candidates) { [secondary] }
          it 'returns empty array' do
            expect(pref.select_nodes(candidates)).to be_empty
          end
        end

        context 'matching secondary' do
          let(:candidates) { [matching_secondary] }
          it 'returns array with matching node' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end
      end

      context 'multiple candidates' do
        context 'no matching secondaries' do
          let(:candidates) do
            [primary, secondary, secondary]
          end

          it 'returns array with primary' do
            expect(pref.select_nodes(candidates)).to eq([primary])
          end
        end

        context 'one matching secondary' do
          let(:candidates) do
            [primary, matching_secondary, secondary]
          end

          it 'returns array with matching secondary then primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [matching_secondary, primary]
            )
          end
        end

        context 'two matching secondaries' do
          let(:candidates) do
            [primary, matching_secondary, matching_secondary]
          end

          it 'retuns array both matching secondaries and primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [matching_secondary] * 2 << primary
            )
          end
        end

        context 'one matching primary and one matching secondary' do
          let(:candidates) do
            [matching_primary, matching_secondary, secondary]
          end

          it 'returns array secondary then primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [matching_secondary, matching_primary]
            )
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
          it 'returns array with candidate' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }
          it 'returns array with candidate' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end
      end

      context 'multiple candidates' do
        context 'local primary, local secondary' do
          let(:candidates) { [primary, secondary] }
          it 'returns an array with secondary then primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [secondary, primary]
            )
          end
        end

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }
          it 'returns array with secondary then primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [far_secondary, primary]
            )
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, secondary] }
          it 'returns array with secondary then primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [secondary, far_primary]
            )
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }

          it 'returns array with secondary then primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [far_secondary, far_primary]
            )
          end
        end

        context 'two near nodes, one far node' do
          context 'near primary, near secondary' do
            let(:candidates) { [primary, secondary, far_secondary] }

            it 'returns array with secondary then primary' do
              expect(pref.select_nodes(candidates)).to eq(
                [secondary, primary]
              )
            end
          end

          context 'two near secondaries' do
            let(:candidates) { [far_primary, secondary, secondary] }

            it 'returns array with secondaries then primary' do
              expect(pref.select_nodes(candidates)).to match_array(
                [secondary, secondary, far_primary]
              )
            end
          end
        end
      end
    end
  end
end

