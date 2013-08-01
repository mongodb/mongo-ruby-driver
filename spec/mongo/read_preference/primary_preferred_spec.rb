require 'spec_helper'

describe Mongo::ReadPreference::PrimaryPreferred do
  include_context 'read preference'

  describe '#name' do
    it 'returns the name' do
      expect(pref.name).to eq(:primary_preferred)
    end
  end

  describe '#tag_sets' do
    context 'tags not provided' do
      it 'returns an empty array' do
        expect(pref.tag_sets).to be_empty
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      it 'returns the tag sets' do
        expect(pref.tag_sets).to eq(tag_sets)
      end
    end
  end

  describe '#to_mongos' do
    it 'returns mongos readPreference' do
      expect(pref.to_mongos).to eq(
        { :mode => 'primaryPreferred' }
      )
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      it 'returns mongos readPreference with tag sets' do
        expect(pref.to_mongos).to eq(
          { :mode => 'primaryPreferred', :tags => tag_sets }
        )
      end
    end
  end

  describe '#select_node' do
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
      it 'returns array with primary then secondary' do
        expect(pref.select_nodes(candidates)).to eq(candidates)
      end
    end

    context 'multiple secondary candidates' do
      let(:candidates) { [primary, secondary, secondary] }
      it 'returns array with primary then secondary' do
        expect(pref.select_nodes(candidates)).to eq(candidates)
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      let(:matching_primary) do
        node(:primary, :tags => tag_set)
      end
      let(:matching_secondary) do
        node(:secondary, :tags => tag_set)
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

        context 'one matching primary' do
          let(:candidates) do
            [matching_primary, secondary, secondary]
          end
          it 'returns array with primary' do
            expect(pref.select_nodes(candidates)).to eq([matching_primary])
          end
        end

        context 'one matching secondary' do
          let(:candidates) do
            [primary, matching_secondary, secondary]
          end

          it 'returns array with matching secondary and primary' do
            expect(pref.select_nodes(candidates)).to eq(
              [primary, matching_secondary]
            )
          end
        end

        context 'two matching secondaries' do
          let(:candidates) do
            [primary, matching_secondary, matching_secondary]
          end

          it 'retuns array both matching secondaries and primary' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end
      end

      context 'one matching primary and one matching secondary' do
        let(:candidates) do
          [matching_primary, matching_secondary, secondary]
        end

        it 'returns array with primary then matching secondary' do
          expect(pref.select_nodes(candidates)).to eq(
            [matching_primary, matching_secondary]
          )
        end
      end
    end

    context 'high latency candidates' do
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
          it 'returns an array with primary then secondary' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }
          it 'returns array with primary then secondary' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, secondary] }
          it 'returns array with primary then secondary' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }
          it 'returns array with primary then secondary' do
            expect(pref.select_nodes(candidates)).to eq(candidates)
          end
        end

        context 'two near nodes, one far node' do
          context 'near primary, near secondary' do
            let(:candidates) { [primary, secondary, far_secondary] }

            it 'returns array with primary then secondary' do
              expect(pref.select_nodes(candidates)).to eq(
                [primary, secondary]
              )
            end
          end

          context 'two near secondaries' do
            let(:candidates) { [far_primary, secondary, secondary] }

            it 'returns array with primary then two secondaries' do
              expect(pref.select_nodes(candidates)).to eq(candidates)
            end
          end
        end
      end
    end
  end
end
