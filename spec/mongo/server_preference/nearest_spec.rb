require 'spec_helper'

describe Mongo::ServerPreference::Nearest do
  include_context 'server preference'

  it_behaves_like 'a server preference mode' do
    let(:name) { :nearest }
    let(:slave_ok) { true }
  end

  it_behaves_like 'a server preference mode accepting tag sets'

  describe '#to_mongos' do

    context 'tag set not provided' do
      it 'returns a server preference formatted for mongos' do
        expect(server_pref.to_mongos).to eq({ :mode => 'nearest' })
      end
    end

    context 'tag set provided' do
      let(:tag_sets) { [tag_set] }

      it 'returns a server preference formatted for mongos' do
        expect(server_pref.to_mongos).to eq(
          { :mode => 'nearest', :tags => tag_sets}
        )
      end
    end
  end

  describe '#select_servers' do

    context 'no candidates' do
      let(:candidates) { [] }

      it 'returns an empty array' do
        expect(server_pref.select_servers(candidates)).to be_empty
      end
    end

    context 'single primary candidates' do
      let(:candidates) { [primary] }

      it 'returns an array with the primary' do
        expect(server_pref.select_servers(candidates)).to eq([primary])
      end
    end

    context 'single secondary candidate' do
      let(:candidates) { [secondary] }

      it 'returns an array with the secondary' do
        expect(server_pref.select_servers(candidates)).to eq([secondary])
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [primary, secondary] }

      it 'returns an array with the primary and secondary' do
        expect(server_pref.select_servers(candidates)).to match_array([primary, secondary])
      end
    end

    context 'multiple secondary candidates' do
      let(:candidates) { [secondary, secondary] }

      it 'returns an array with the secondaries' do
        expect(server_pref.select_servers(candidates)).to match_array([secondary, secondary])
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      let(:matching_primary) do
        server(:primary, :tags => tag_sets)
      end
      let(:matching_secondary) do
        server(:secondary, :tags => tag_sets)
      end

      context 'single candidate' do

        context 'primary' do
          let(:candidates) { [primary] }

          it 'returns an empty array' do
            expect(server_pref.select_servers(candidates)).to be_empty
          end
        end

        context 'matching primary' do
          let(:candidates) { [matching_primary] }

          it 'returns an array with the primary' do
            expect(server_pref.select_servers(candidates)).to eq([matching_primary])
          end
        end

        context 'secondary' do
          let(:candidates) { [secondary] }

          it 'returns an empty array' do
            expect(server_pref.select_servers(candidates)).to be_empty
          end
        end

        context 'matching secondary' do
          let(:candidates) { [matching_secondary] }

          it 'returns an array with the matching secondary' do
            expect(server_pref.select_servers(candidates)).to eq([matching_secondary])
          end
        end
      end

      context 'mtuliple candidates' do

        context 'no matching servers' do
          let(:candidates) { [primary, secondary, secondary] }

          it 'returns an empty array' do
            expect(server_pref.select_servers(candidates)).to be_empty
          end
        end

        context 'one matching primary' do
          let(:candidates) { [matching_primary, secondary, secondary] }

          it 'returns an array with the matching primary' do
            expect(server_pref.select_servers(candidates)).to eq([matching_primary])
          end
        end

        context 'one matching secondary' do
          let(:candidates) { [primary, matching_secondary, secondary] }

          it 'returns an array with the matching secondary' do
            expect(server_pref.select_servers(candidates)).to eq([matching_secondary])
          end
        end

        context 'two matching secondaries' do
          let(:candidates) { [primary, matching_secondary, matching_secondary] }
          let(:expected) { [matching_secondary, matching_secondary] }

          it 'returns an array with the matching secondaries' do
            expect(server_pref.select_servers(candidates)).to eq(expected)
          end
        end

        context 'one matching primary and one matching secondary' do
          let(:candidates) { [matching_primary, matching_secondary, secondary] }
          let(:expected) { [matching_primary, matching_secondary] }

          it 'returns an array with the matching primary and secondary' do
            expect(server_pref.select_servers(candidates)).to match_array(expected)
          end
        end
      end
    end

    context 'high latency servers' do
      let(:far_primary) { server(:primary, :ping => 100) }
      let(:far_secondary) { server(:secondary, :ping => 113) }

      context 'single candidate' do

        context 'far primary' do
          let(:candidates) { [far_primary] }

          it 'returns array with far primary' do
            expect(server_pref.select_servers(candidates)).to eq([far_primary])
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns array with far primary' do
            expect(server_pref.select_servers(candidates)).to eq([far_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'local primary, local secondary' do
          let(:candidates) { [primary, secondary] }

          it 'returns array with primary and secondary' do
            expect(server_pref.select_servers(candidates)).to match_array(
              [primary, secondary]
            )
          end
        end

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }

          # @todo: is this right?
          it 'returns array with local primary' do
            expect(server_pref.select_servers(candidates)).to eq([primary])
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, secondary] }

          it 'returns array with local secondary' do
            expect(server_pref.select_servers(candidates)).to eq([secondary])
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }
          let(:expected) { [far_primary, far_secondary] }

          it 'returns array with both servers' do
            expect(server_pref.select_servers(candidates)).to match_array(expected)
          end
        end

        context 'two local servers, one far server' do

          context 'local primary, local secondary' do
            let(:candidates) { [primary, secondary, far_secondary] }
            let(:expected) { [primary, secondary] }

            it 'returns array with local primary and local secondary' do
              expect(server_pref.select_servers(candidates)).to match_array(expected)
            end
          end

          context 'two near secondaries' do
            let(:candidates) { [far_primary, secondary, secondary] }
            let(:expected) { [secondary, secondary] }

            it 'returns array with the two local secondaries' do
              expect(server_pref.select_servers(candidates)).to match_array(expected)
            end
          end
        end
      end
    end
  end
end
