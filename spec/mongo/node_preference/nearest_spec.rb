require 'spec_helper'

describe Mongo::NodePreference::Nearest do
  include_context 'node preference'

  it_behaves_like 'a node preference mode' do
    let(:name) { :nearest }
    let(:slave_ok) { true }
  end

  it_behaves_like 'a node preference mode accepting tag sets'

  describe '#to_mongos' do

    context 'tag set not provided' do
      it 'returns a node preference formatted for mongos' do
        expect(read_pref.to_mongos).to eq({ :mode => 'nearest' })
      end
    end

    context 'tag set provided' do
      let(:tag_sets) { [tag_set] }

      it 'returns a node preference formatted for mongos' do
        expect(read_pref.to_mongos).to eq(
          { :mode => 'nearest', :tags => tag_sets}
        )
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

      it 'returns an array with the primary' do
        expect(read_pref.select_nodes(candidates)).to eq([primary])
      end
    end

    context 'single secondary candidate' do
      let(:candidates) { [secondary] }

      it 'returns an array with the secondary' do
        expect(read_pref.select_nodes(candidates)).to eq([secondary])
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [primary, secondary] }

      it 'returns an array with the primary and secondary' do
        expect(read_pref.select_nodes(candidates)).to match_array([primary, secondary])
      end
    end

    context 'multiple secondary candidates' do
      let(:candidates) { [secondary, secondary] }

      it 'returns an array with the secondaries' do
        expect(read_pref.select_nodes(candidates)).to match_array([secondary, secondary])
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

          it 'returns an empty array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
          end
        end

        context 'matching primary' do
          let(:candidates) { [matching_primary] }

          it 'returns an array with the primary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_primary])
          end
        end

        context 'secondary' do
          let(:candidates) { [secondary] }

          it 'returns an empty array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
          end
        end

        context 'matching secondary' do
          let(:candidates) { [matching_secondary] }

          it 'returns an array with the matching secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_secondary])
          end
        end
      end

      context 'mtuliple candidates' do

        context 'no matching nodes' do
          let(:candidates) { [primary, secondary, secondary] }

          it 'returns an empty array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
          end
        end

        context 'one matching primary' do
          let(:candidates) { [matching_primary, secondary, secondary] }

          it 'returns an array with the matching primary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_primary])
          end
        end

        context 'one matching secondary' do
          let(:candidates) { [primary, matching_secondary, secondary] }

          it 'returns an array with the matching secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_secondary])
          end
        end

        context 'two matching secondaries' do
          let(:candidates) { [primary, matching_secondary, matching_secondary] }
          let(:expected) { [matching_secondary, matching_secondary] }

          it 'returns an array with the matching secondaries' do
            expect(read_pref.select_nodes(candidates)).to eq(expected)
          end
        end

        context 'one matching primary and one matching secondary' do
          let(:candidates) { [matching_primary, matching_secondary, secondary] }
          let(:expected) { [matching_primary, matching_secondary] }

          it 'returns an array with the matching primary and secondary' do
            expect(read_pref.select_nodes(candidates)).to match_array(expected)
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

          it 'returns array with far primary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_primary])
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns array with far primary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'local primary, local secondary' do
          let(:candidates) { [primary, secondary] }

          it 'returns array with primary and secondary' do
            expect(read_pref.select_nodes(candidates)).to match_array(
              [primary, secondary]
            )
          end
        end

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }

          # @todo: is this right?
          it 'returns array with local primary' do
            expect(read_pref.select_nodes(candidates)).to eq([primary])
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, secondary] }

          it 'returns array with local secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([secondary])
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }
          let(:expected) { [far_primary, far_secondary] }

          it 'returns array with both nodes' do
            expect(read_pref.select_nodes(candidates)).to match_array(expected)
          end
        end

        context 'two local nodes, one far node' do

          context 'local primary, local secondary' do
            let(:candidates) { [primary, secondary, far_secondary] }
            let(:expected) { [primary, secondary] }

            it 'returns array with local primary and local secondary' do
              expect(read_pref.select_nodes(candidates)).to match_array(expected)
            end
          end

          context 'two near secondaries' do
            let(:candidates) { [far_primary, secondary, secondary] }
            let(:expected) { [secondary, secondary] }

            it 'returns array with the two local secondaries' do
              expect(read_pref.select_nodes(candidates)).to match_array(expected)
            end
          end
        end
      end
    end
  end
end
