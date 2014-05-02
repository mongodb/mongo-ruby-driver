require 'spec_helper'

describe Mongo::NodePreference::Secondary do
  include_context 'node preference'

  it_behaves_like 'a node preference mode' do
    let(:name) { :secondary }
    let(:slave_ok) { true }
  end

  it_behaves_like 'a node preference mode accepting tag sets'

  describe '#to_mongos' do

    it 'returns node preference formatted for mongos' do
      expect(read_pref.to_mongos).to eq(
        { :mode => 'secondary' }
      )
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }

      it 'returns node preference formatted for mongos with tag sets' do
        expect(read_pref.to_mongos).to eq(
          { :mode => 'secondary', :tags => tag_sets}
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

    context 'single primary candidate' do
      let(:candidates) { [primary] }

      it 'returns an empty array' do
        expect(read_pref.select_nodes(candidates)).to be_empty
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

      it 'returns array with secondary' do
        expect(read_pref.select_nodes(candidates)).to eq([secondary])
      end
    end

    context 'multiple secondary candidates' do
      let(:candidates) { [secondary, secondary, primary] }

      it 'returns array with all secondaries' do
        expect(read_pref.select_nodes(candidates)).to eq([secondary, secondary])
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      let(:matching_secondary) { node(:secondary, :tags => tag_sets) }

      context 'single candidate' do

        context 'primary' do
          let(:candidates) { [primary] }

          it 'returns an empty array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
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

          it 'returns an array with matching secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'no matching candidates' do
          let(:candidates) { [primary, secondary, secondary] }

          it 'returns an emtpy array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
          end
        end

        context 'one matching secondary' do
          let(:candidates) { [secondary, matching_secondary]}

          it 'returns array with matching secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_secondary])
          end
        end

        context 'two matching secondaries' do
          let(:candidates) { [matching_secondary, matching_secondary] }

          it 'returns an array with both matching secondaries' do
            expect(read_pref.select_nodes(candidates)).to eq([matching_secondary, matching_secondary])
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

          it 'returns an empty array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
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

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }

          it 'returns an array with the secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_secondary])
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }

          it 'returns an array with the secondary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_secondary])
          end
        end

        context 'two near nodes, one far node' do

          context 'near primary, near and far secondaries' do
            let(:candidates) { [primary, secondary, far_secondary] }

            it 'returns an array with near secondary' do
              expect(read_pref.select_nodes(candidates)).to eq([secondary])
            end
          end

          context 'far primary and two near secondaries' do
            let(:candidates) { [far_primary, secondary, secondary] }

            it 'returns an array with two secondaries' do
              expect(read_pref.select_nodes(candidates)).to eq([secondary, secondary])
            end
          end
        end
      end
    end
  end
end