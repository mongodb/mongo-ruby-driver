require 'spec_helper'

describe Mongo::NodePreference::Primary do

  include_context 'node preference'

  it_behaves_like 'a node preference mode' do
    let(:name) { :primary }
    let(:slave_ok) { false }
  end

  describe '#tag_sets' do

    context 'tags not provided' do

      it 'returns an empty array' do
        expect(read_pref.tag_sets).to be_empty
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }

      it 'raises an error' do
        expect{read_pref.tag_sets}.to raise_error
      end
    end
  end

  describe '#to_mongos' do

    it 'returns nil' do
      expect(read_pref.to_mongos).to be_nil
    end
  end

  describe '#select_nodes' do

    context 'no candidates' do
      let(:candidates) { [] }

      it 'returns an empty array' do
        expect(read_pref.select_nodes(candidates)).to be_empty
      end
    end

    context 'secondary candidates' do
      let(:candidates) { [secondary] }

      it 'returns an empty array' do
        expect(read_pref.select_nodes(candidates)).to be_empty
      end
    end

    context 'primary candidate' do
      let(:candidates) { [primary] }

      it 'returns an array with the primary' do
        expect(read_pref.select_nodes(candidates)).to eq([primary])
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [secondary, primary] }

      it 'returns an array with the primary' do
        expect(read_pref.select_nodes(candidates)).to eq([primary])
      end
    end

    context 'high latency candidates' do
      let(:far_primary) { node(:primary, :ping => 100) }
      let(:far_secondary) { node(:secondary, :ping => 120) }

      context 'single candidate' do

        context 'far primary' do
          let(:candidates) { [far_primary] }

          it 'returns array with the primary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_primary])
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns empty array' do
            expect(read_pref.select_nodes(candidates)).to be_empty
          end
        end
      end

      context 'multiple candidates' do

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }

          it 'returns an array with the primary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_primary])
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, far_secondary] }

          it 'returns an array with the primary' do
            expect(read_pref.select_nodes(candidates)).to eq([far_primary])
          end
        end
      end
    end
  end
end
