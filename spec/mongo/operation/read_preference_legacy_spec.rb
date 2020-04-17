require 'spec_helper'

describe Mongo::Operation::ReadPreferenceSupported do

  let(:selector) do
    { name: 'test' }
  end

  let(:options) do
    {}
  end

  let(:cluster) do
    double('cluster').tap do |cluster|
      allow(cluster).to receive(:single?).and_return(single?)
    end
  end

  let(:operation) do
    Class.new do
      include Mongo::Operation::ReadPreferenceSupported
    end.new.tap do |op|
      allow(op).to receive(:read).and_return(read_pref)
      allow(op).to receive(:selector).and_return(selector)
      allow(op).to receive(:options).and_return(options)
    end
  end

  let(:server) do
    double('server').tap do |server|
      allow(server).to receive(:cluster).and_return(cluster)
      allow(server).to receive(:mongos?).and_return(mongos?)
      allow(server).to receive(:standalone?).and_return(standalone?)
    end
  end

  let(:connection) do
    double('connection').tap do |connection|
      allow(connection).to receive(:server).and_return(server)
    end
  end

  describe '#add_slave_ok_flag_maybe' do

    let(:actual) do
      operation.send(:add_slave_ok_flag_maybe, operation.send(:options), connection)
    end

    shared_examples_for 'sets the slave_ok flag as expected' do
      it 'sets the slave_ok flag as expected' do
        expect(actual).to eq(expected)
      end
    end

    shared_examples_for 'never sets slave_ok' do

      let(:expected) do
        { }
      end

      context 'when no read preference is specified' do
        let(:read_pref) { Mongo::ServerSelector.get }

        it_behaves_like 'sets the slave_ok flag as expected'
      end

      context 'when primary read preference is specified' do
        let(:read_pref) { Mongo::ServerSelector.get(:mode => :primary) }

        it_behaves_like 'sets the slave_ok flag as expected'
      end

      context 'when secondary read preference is specified' do
        let(:read_pref) { Mongo::ServerSelector.get(:mode => :secondary) }

        it_behaves_like 'sets the slave_ok flag as expected'
      end
    end

    shared_examples_for 'always sets slave_ok' do

      let(:expected) do
        { :flags => [ :slave_ok ] }
      end

      context 'when no read preference is specified' do
        let(:read_pref) { Mongo::ServerSelector.get }

        it_behaves_like 'sets the slave_ok flag as expected'
      end

      context 'when primary read preference is specified' do
        let(:read_pref) { Mongo::ServerSelector.get(:mode => :primary) }

        it_behaves_like 'sets the slave_ok flag as expected'
      end

      context 'when secondary read preference is specified' do
        let(:read_pref) { Mongo::ServerSelector.get(:mode => :secondary) }

        it_behaves_like 'sets the slave_ok flag as expected'
      end
    end

    shared_examples_for 'sets slave_ok if read preference is specified and is not primary' do

      context 'when there is no read preference set' do

        let(:read_pref) { Mongo::ServerSelector.get }

        let(:expected) do
          { }
        end

        it_behaves_like 'sets the slave_ok flag as expected'
      end

      context 'when there is a read preference' do

        context 'when the read preference requires the slave_ok flag' do

          let(:read_pref) { Mongo::ServerSelector.get(:mode => :secondary) }

          let(:expected) do
            { :flags => [ :slave_ok ] }
          end

          it_behaves_like 'sets the slave_ok flag as expected'
        end

        context 'when the read preference does not require the slave_ok flag' do

          let(:read_pref) { Mongo::ServerSelector.get(:mode => :primary) }

          let(:expected) do
            { }
          end

          it_behaves_like 'sets the slave_ok flag as expected'
        end
      end
    end

    context 'when the topology is Single' do

      let(:single?) { true }
      let(:mongos?) { false }

      context 'when the server is a standalone' do

        let(:standalone?) { true }

        it_behaves_like 'never sets slave_ok'
      end

      context 'when the server is a mongos' do

        let(:standalone?) { false }
        let(:mongos?) { true }

        it_behaves_like 'always sets slave_ok'
      end

      context 'when the server is a replica set member' do

        let(:standalone?) { false }
        let(:mongos?) { false }

        it_behaves_like 'always sets slave_ok'
      end
    end

    context 'when the topology is not Single' do

      let(:single?) { false }
      let(:mongos?) { false }

      context 'when the server is a standalone' do

        let(:standalone?) { true }

        it_behaves_like 'never sets slave_ok'
      end

      context 'when the server is a mongos' do

        let(:standalone?) { false }
        let(:mongos?) { true }

        it_behaves_like 'sets slave_ok if read preference is specified and is not primary'
      end

      context 'when the server is a replica set member' do

        let(:standalone?) { false }
        let(:mongos?) { false }

        it_behaves_like 'sets slave_ok if read preference is specified and is not primary'
      end
    end
  end

  describe '#update_selector_for_read_pref' do

    let(:read_pref) do
      Mongo::ServerSelector.get(:mode => mode)
    end

    # Behavior of sending $readPreference is the same regardless of topology.
    shared_examples_for '$readPreference in the command' do
      let(:actual) do
        operation.send(:update_selector_for_read_pref, operation.send(:selector), connection)
      end

      let(:expected_read_preference) do
        {mode: mode.to_s.gsub(/_(.)/) { $1.upcase }}
      end

      shared_examples_for 'adds read preference moving existing contents to $query' do

        let(:expected) do
          { :$query => selector, :$readPreference => expected_read_preference }
        end

        it 'moves existing selector contents under $query and adds read preference' do
          expect(actual).to eq(expected)
        end

        context 'when the selector already has $query in it' do

          let(:selector) do
            { :$query => { :name => 'test' },
              :$orderby => { :name => -1 } }
          end

          let(:expected) do
            selector.merge(:$readPreference => expected_read_preference)
          end

          it 'keeps existing $query and adds read preference' do
            expect(actual).to eq(expected)
          end
        end
      end

      shared_examples_for 'does not modify selector' do

        it 'does not modify selector' do
          expect(actual).to eq(selector)
        end
      end

      shared_examples_for 'does not send read preference' do
        ([nil] + %i(primary primary_preferred secondary secondary_preferred nearest)).each do |_mode|
          active_mode = _mode

          context "when read preference mode is #{active_mode}" do
            let(:mode) { active_mode }

            it_behaves_like 'does not modify selector'
          end
        end
      end

      context 'when the server is a standalone' do

        let(:standalone?) { true }
        let(:mongos?) { false }

        it_behaves_like 'does not send read preference'
      end

      context 'when the server is a mongos' do

        let(:standalone?) { false }
        let(:mongos?) { true }

        context 'when the read preference mode is nil' do

          let(:mode) { nil }

          it_behaves_like 'does not modify selector'
        end

        context 'when the read preference mode is primary' do

          let(:mode) { :primary }

          it_behaves_like 'does not modify selector'
        end

        context 'when the read preference mode is primary_preferred' do

          let(:mode) { :primary_preferred }

          it_behaves_like 'adds read preference moving existing contents to $query'
        end

        context 'when the read preference mode is secondary' do

          let(:mode) { :secondary }

          it_behaves_like 'adds read preference moving existing contents to $query'
        end

        context 'when the read preference mode is secondary_preferred' do

          let(:mode) { :secondary_preferred }

          it_behaves_like 'does not modify selector'

          context 'when there are fields in the selector besides :mode' do
            let(:read_pref) do
              Mongo::ServerSelector.get(:mode => mode, tag_sets: ['dc' => 'nyc'])
            end

            let(:expected_read_preference) do
              {mode: mode.to_s.gsub(/_(.)/) { $1.upcase }, tags: ['dc' => 'nyc']}
            end

            it_behaves_like 'adds read preference moving existing contents to $query'
          end
        end

        context 'when the read preference mode is nearest' do

          let(:mode) { :nearest }

          it_behaves_like 'adds read preference moving existing contents to $query'
        end
      end

      context 'when the server is a replica set member' do

        let(:standalone?) { false }
        let(:mongos?) { false }

        # $readPreference is not sent to replica set nodes running legacy
        # servers - the allowance of secondary reads is handled by slave_ok
        # flag.
        it_behaves_like 'does not send read preference'
      end
    end

    context 'in single topology' do
      let(:single?) { true }

      it_behaves_like '$readPreference in the command'
    end

    context 'not in single topology' do
      let(:single?) { false }

      it_behaves_like '$readPreference in the command'
    end
  end
end
