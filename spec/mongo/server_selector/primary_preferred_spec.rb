require 'spec_helper'

describe Mongo::ServerSelector::PrimaryPreferred do

  let(:name) { :primary_preferred }

  include_context 'server selector'

  it_behaves_like 'a server selector mode' do
    let(:slave_ok) { true }
  end

  it_behaves_like 'a server selector accepting tag sets'
  it_behaves_like 'a server selector with sensitive data in its options'

  describe '#to_mongos' do

    context 'tag sets not provided' do

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq({ :mode => 'primaryPreferred' })
      end
    end

    context 'tag set provided' do
      let(:tag_sets) { [tag_set] }

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq(
          { :mode => 'primaryPreferred', :tags => tag_sets}
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

      it 'returns an array with the primary' do
        expect(selector.send(:select, candidates)).to eq( [primary] )
      end
    end

    context 'single secondary candidate' do
      let(:candidates) { [secondary] }

      it 'returns an array with the secondary' do
        expect(selector.send(:select, candidates)).to eq( [secondary] )
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [secondary, primary] }
      let(:expected) { [primary] }

      it 'returns an array with the primary' do
        expect(selector.send(:select, candidates)).to eq(expected)
      end
    end

    context 'secondary and primary candidates' do
      let(:candidates) { [secondary, primary] }
      let(:expected) { [primary] }

      it 'returns an array with the primary' do
        expect(selector.send(:select, candidates)).to eq(expected)
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }

      let(:matching_primary) do
        server(:primary, :tags => server_tags )
      end

      let(:matching_secondary) do
        server(:secondary, :tags => server_tags )
      end

      context 'single candidate' do

        context 'primary' do
          let(:candidates) { [primary] }

          it 'returns array with primary' do
            expect(selector.send(:select, candidates)).to eq([primary])
          end
        end

        context 'matching_primary' do
          let(:candidates) { [matching_primary] }

          it 'returns array with matching primary' do
            expect(selector.send(:select, candidates)).to eq([matching_primary])
          end
        end

        context 'matching secondary' do
          let(:candidates) { [matching_secondary] }

          it 'returns array with matching secondary' do
            expect(selector.send(:select, candidates)).to eq([matching_secondary])
          end
        end

        context 'secondary' do
          let(:candidates) { [secondary] }

          it 'returns an empty array' do
            expect(selector.send(:select, candidates)).to be_empty
          end
        end
      end

      context 'multiple candidates' do

        context 'no matching secondaries' do
          let(:candidates) { [primary, secondary, secondary] }

          it 'returns an array with the primary' do
            expect(selector.send(:select, candidates)).to eq([primary])
          end
        end

        context 'one matching primary' do
          let(:candidates) { [matching_primary, secondary, secondary] }

          it 'returns an array of the primary' do
            expect(selector.send(:select, candidates)).to eq([matching_primary])
          end
        end

        context 'one matching secondary' do
          let(:candidates) { [primary, matching_secondary, secondary] }
          let(:expected) { [primary] }

          it 'returns an array of the primary' do
            expect(selector.send(:select, candidates)).to eq(expected)
          end
        end

        context 'two matching secondaries' do
          let(:candidates) { [primary, matching_secondary, matching_secondary] }
          let(:expected) { [primary] }

          it 'returns an array of the primary ' do
            expect(selector.send(:select, candidates)).to eq(expected)
          end
        end

        context 'one matching primary, one matching secondary' do
          let(:candidates) { [primary, matching_secondary, secondary] }
          let(:expected) { [primary] }

          it 'returns an array of the primary' do
            expect(selector.send(:select, candidates)).to eq(expected)
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

          it 'returns array with far primary' do
            expect(selector.send(:select, candidates)).to eq([far_primary])
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns array with far primary' do
            expect(selector.send(:select, candidates)).to eq([far_secondary])
          end

        end
      end

      context 'multiple candidates' do

        context 'primary available' do

          context 'local primary, local secondary' do
            let(:candidates) { [primary, secondary] }
            let(:expected) { [primary] }

            it 'returns an array of the primary' do
              expect(selector.send(:select, candidates)).to eq(expected)
            end
          end

          context 'local primary, far secondary' do
            let(:candidates) { [primary, far_secondary] }
            let(:expected) { [primary] }

            it 'returns an array of the primary' do
              expect(selector.send(:select, candidates)).to eq(expected)
            end
          end

          context 'far primary, local secondary' do
            let(:candidates) { [far_primary, secondary] }
            let(:expected) { [far_primary] }

            it 'returns an array of the far primary' do
              expect(selector.send(:select, candidates)).to eq(expected)
            end
          end

          context 'far primary, far secondary' do
            let(:candidates) { [far_primary, far_secondary] }
            let(:expected) { [far_primary] }

            it 'returns an array of the far primary' do
              expect(selector.send(:select, candidates)).to eq(expected)
            end
          end

          context 'two local servers, one far server' do

            context 'local primary, local secondary, far secondary' do
              let(:candidates) { [primary, secondary, far_secondary] }
              let(:expected) { [primary] }

              it 'returns an array of the primary' do
                expect(selector.send(:select, candidates)).to eq(expected)
              end
            end

            context 'two local secondaries' do
              let(:candidates) { [far_primary, secondary, secondary] }
              let(:expected) { [far_primary] }

              it 'returns an array with primary' do
                expect(selector.send(:select, candidates)).to eq(expected)
              end
            end
          end
        end

        context 'primary not available' do

          context 'one secondary' do
            let(:candidates) { [secondary] }
            let(:expected) { [secondary] }

            it 'returns an array with the secondary' do
              expect(selector.send(:select, candidates)).to eq(expected)
            end
          end

          context 'one local secondary, one far secondary' do
            let(:candidates) { [secondary, far_secondary] }
            let(:expected) { [secondary] }

            it 'returns an array of the secondary' do
              expect(selector.send(:select, candidates)).to eq(expected)
            end
          end

          context 'two local secondaries, one far secondary' do
            let(:candidates) { [secondary, secondary, far_secondary] }
            let(:expected) { [secondary, secondary] }

            it 'returns an array of the secondary' do
              expect(selector.send(:select, candidates)).to eq(expected)
            end
          end
        end
      end
    end
  end
end

