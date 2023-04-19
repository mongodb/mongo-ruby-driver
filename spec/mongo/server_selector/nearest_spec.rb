# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require 'support/shared/server_selector'

describe Mongo::ServerSelector::Nearest do

  let(:name) { :nearest }

  include_context 'server selector'

  let(:default_address) { 'test.host' }

  it_behaves_like 'a server selector mode' do
    let(:secondary_ok) { true }
  end

  it_behaves_like 'a server selector accepting tag sets'
  it_behaves_like 'a server selector accepting hedge'
  it_behaves_like 'a server selector with sensitive data in its options'

  describe '#initialize' do

    context 'when max_staleness is provided' do

      let(:options) do
        { max_staleness: 95 }
      end

      it 'sets the max_staleness option' do
        expect(selector.max_staleness).to eq(options[:max_staleness])
      end
    end
  end

  describe '#==' do

    context 'when max staleness is the same' do

      let(:options) do
        { max_staleness: 95 }
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
        { max_staleness: 100 }
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

    context 'tag set not provided' do

      let(:expected) do
        { :mode => 'nearest' }
      end

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq(expected)
      end
    end

    context 'tag set provided' do

      let(:tag_sets) do
        [tag_set]
      end

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq(
          { :mode => 'nearest', :tags => tag_sets }
        )
      end
    end

    context 'max staleness not provided' do

      let(:expected) do
        { :mode => 'nearest' }
      end

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq(expected)
      end
    end

    context 'max staleness provided' do

      let(:max_staleness) do
        100
      end

      let(:expected) do
        { :mode => 'nearest', maxStalenessSeconds: 100 }
      end

      it 'returns a read preference formatted for mongos' do
        expect(selector.to_mongos).to eq(expected)
      end
    end
  end

  describe '#select_in_replica_set' do

    context 'no candidates' do
      let(:candidates) { [] }

      it 'returns an empty array' do
        expect(selector.send(:select_in_replica_set, candidates)).to be_empty
      end
    end

    context 'single primary candidates' do
      let(:candidates) { [primary] }

      it 'returns an array with the primary' do
        expect(selector.send(:select_in_replica_set, candidates)).to eq([primary])
      end
    end

    context 'single secondary candidate' do
      let(:candidates) { [secondary] }

      it 'returns an array with the secondary' do
        expect(selector.send(:select_in_replica_set, candidates)).to eq([secondary])
      end
    end

    context 'primary and secondary candidates' do
      let(:candidates) { [primary, secondary] }

      it 'returns an array with the primary and secondary' do
        expect(selector.send(:select_in_replica_set, candidates)).to match_array([primary, secondary])
      end
    end

    context 'multiple secondary candidates' do
      let(:candidates) { [secondary, secondary] }

      it 'returns an array with the secondaries' do
        expect(selector.send(:select_in_replica_set, candidates)).to match_array([secondary, secondary])
      end
    end

    context 'tag sets provided' do
      let(:tag_sets) { [tag_set] }
      let(:matching_primary) do
        make_server(:primary, :tags => server_tags, address: default_address)
      end
      let(:matching_secondary) do
        make_server(:secondary, :tags => server_tags, address: default_address)
      end

      context 'single candidate' do

        context 'primary' do
          let(:candidates) { [primary] }

          it 'returns an empty array' do
            expect(selector.send(:select_in_replica_set, candidates)).to be_empty
          end
        end

        context 'matching primary' do
          let(:candidates) { [matching_primary] }

          it 'returns an array with the primary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([matching_primary])
          end
        end

        context 'secondary' do
          let(:candidates) { [secondary] }

          it 'returns an empty array' do
            expect(selector.send(:select_in_replica_set, candidates)).to be_empty
          end
        end

        context 'matching secondary' do
          let(:candidates) { [matching_secondary] }

          it 'returns an array with the matching secondary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([matching_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'no matching servers' do
          let(:candidates) { [primary, secondary, secondary] }

          it 'returns an empty array' do
            expect(selector.send(:select_in_replica_set, candidates)).to be_empty
          end
        end

        context 'one matching primary' do
          let(:candidates) { [matching_primary, secondary, secondary] }

          it 'returns an array with the matching primary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([matching_primary])
          end
        end

        context 'one matching secondary' do
          let(:candidates) { [primary, matching_secondary, secondary] }

          it 'returns an array with the matching secondary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([matching_secondary])
          end
        end

        context 'two matching secondaries' do
          let(:candidates) { [primary, matching_secondary, matching_secondary] }
          let(:expected) { [matching_secondary, matching_secondary] }

          it 'returns an array with the matching secondaries' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq(expected)
          end
        end

        context 'one matching primary and one matching secondary' do
          let(:candidates) { [matching_primary, matching_secondary, secondary] }
          let(:expected) { [matching_primary, matching_secondary] }

          it 'returns an array with the matching primary and secondary' do
            expect(selector.send(:select_in_replica_set, candidates)).to match_array(expected)
          end
        end
      end
    end

    context 'high latency servers' do
      let(:far_primary) { make_server(:primary, :average_round_trip_time => 0.113, address: default_address) }
      let(:far_secondary) { make_server(:secondary, :average_round_trip_time => 0.114, address: default_address) }

      context 'single candidate' do

        context 'far primary' do
          let(:candidates) { [far_primary] }

          it 'returns array with far primary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([far_primary])
          end
        end

        context 'far secondary' do
          let(:candidates) { [far_secondary] }

          it 'returns array with far primary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([far_secondary])
          end
        end
      end

      context 'multiple candidates' do

        context 'local primary, local secondary' do
          let(:candidates) { [primary, secondary] }

          it 'returns array with primary and secondary' do
            expect(selector.send(:select_in_replica_set, candidates)).to match_array(
              [primary, secondary]
            )
          end
        end

        context 'local primary, far secondary' do
          let(:candidates) { [primary, far_secondary] }

          it 'returns array with local primary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([primary])
          end
        end

        context 'far primary, local secondary' do
          let(:candidates) { [far_primary, secondary] }

          it 'returns array with local secondary' do
            expect(selector.send(:select_in_replica_set, candidates)).to eq([secondary])
          end
        end

        context 'far primary, far secondary' do
          let(:candidates) { [far_primary, far_secondary] }
          let(:expected) { [far_primary, far_secondary] }

          it 'returns array with both servers' do
            expect(selector.send(:select_in_replica_set, candidates)).to match_array(expected)
          end
        end

        context 'two local servers, one far server' do

          context 'local primary, local secondary' do
            let(:candidates) { [primary, secondary, far_secondary] }
            let(:expected) { [primary, secondary] }

            it 'returns array with local primary and local secondary' do
              expect(selector.send(:select_in_replica_set, candidates)).to match_array(expected)
            end
          end

          context 'two near secondaries' do
            let(:candidates) { [far_primary, secondary, secondary] }
            let(:expected) { [secondary, secondary] }

            it 'returns array with the two local secondaries' do
              expect(selector.send(:select_in_replica_set, candidates)).to match_array(expected)
            end
          end
        end
      end
    end
  end
end
