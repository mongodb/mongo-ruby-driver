require 'spec_helper'

describe Mongo::Operation::ReadPreference do

  let(:selector) do
    { name: 'test' }
  end

  let(:options) do
    {}
  end

  let(:cluster_double) do
    double('cluster')
  end

  let(:single?) do
    true
  end

  let(:mongos?) do
    false
  end

  let(:read_pref) do
    Mongo::ServerSelector.get
  end

  let(:operation) do
    Class.new do
      include Mongo::Operation::ReadPreference
    end.new.tap do |rp|
      allow(rp).to receive(:read).and_return(read_pref)
      allow(rp).to receive(:selector).and_return(selector)
      allow(rp).to receive(:options).and_return(options)
    end
  end

  let(:server) do
    double('server').tap do |c|
      allow(c).to receive(:cluster).and_return(cluster_double)
      allow(cluster_double).to receive(:single?).and_return(single?)
      allow(c).to receive(:mongos?).and_return(mongos?)
    end
  end

  shared_context 'a selector updater' do

    let(:read_pref) do
      Mongo::ServerSelector.get(:mode => mode)
    end

    let(:expected) do
      { :$query => selector, :$readPreference => read_pref.to_mongos }
    end

    it 'returns a special selector' do
      expect(operation.send(:update_selector_for_read_pref, operation.send(:selector), server)).to eq(expected)
    end

    context 'when the selector already has $query in it' do

      let(:selector) do
        { :$query => { :name => 'test' },
          :$orderby => { :name => -1 } }
      end

      let(:expected) do
        selector.merge(:$readPreference => read_pref.to_mongos)
      end

      it 'returns an unaltered special selector' do
        expect(operation.send(:update_selector_for_read_pref, operation.send(:selector), server)).to eq(expected)
      end
    end
  end

  shared_context 'not a selector updater' do

    let(:read_pref) do
      Mongo::ServerSelector.get(:mode => mode)
    end

    it 'returns a selector' do
      expect(operation.send(:update_selector_for_read_pref, operation.send(:selector), server)).to eq(selector)
    end
  end

  context 'when the server is a mongos' do

    let(:mongos?) do
      true
    end

    context 'when the read preference mode is primary' do

      let(:mode) do
        :primary
      end

      it_behaves_like 'not a selector updater'
    end

    context 'when the read preference mode is primary_preferred' do

      let(:mode) do
        :primary_preferred
      end

      it_behaves_like 'a selector updater'
    end

    context 'when the read preference mode is secondary' do

      let(:mode) do
        :secondary
      end

      it_behaves_like 'a selector updater'
    end

    context 'when the read preference mode is secondary_preferred' do

      let(:mode) do
        :secondary_preferred
      end

      it_behaves_like 'not a selector updater'
    end

    context 'when the read preference mode is nearest' do

      let(:mode) do
        :nearest
      end

      it_behaves_like 'a selector updater'
    end
  end

  context 'when the server is not a mongos' do

    let(:mongos?) do
      false
    end

    let(:mode) do
      :secondary_preferred
    end

    it_behaves_like 'not a selector updater'
  end

  context 'when the topology is Single' do

    let(:single?) do
      true
    end

    context 'when the server is a mongos' do

      let(:mongos?) do
        true
      end

      let(:expected) do
        { }
      end

      it 'does not set the slave_ok flag' do
        expect(operation.send(:update_options_for_slave_ok, operation.send(:options), server)).to eq(expected)
      end
    end

    context 'when the server is not a mongos' do

      let(:mongos?) do
        false
      end

      let(:expected) do
        { :flags => [ :slave_ok ] }
      end

      it 'sets the slave_ok flag' do
        expect(operation.send(:update_options_for_slave_ok, operation.send(:options), server)).to eq(expected)
      end
    end
  end

  context 'when the topology is not Single' do

    let(:single?) do
      false
    end

    context 'when there is no read preference set' do

      let(:read_pref) do
        Mongo::ServerSelector.get
      end

      let(:expected) do
        { }
      end

      it 'does not set the slave_ok flag' do
        expect(operation.send(:update_options_for_slave_ok, operation.send(:options), server)).to eq(expected)
      end
    end

    context 'when there is a read preference' do

      context 'when the read preference requires the slave_ok flag' do

        let(:read_pref) do
          Mongo::ServerSelector.get(:mode => :secondary)
        end

        let(:expected) do
          { :flags => [ :slave_ok ] }
        end

        it 'sets the slave_ok flag' do
          expect(operation.send(:update_options_for_slave_ok, operation.send(:options), server)).to eq(expected)
        end
      end

      context 'when the read preference does not require the slave_ok flag' do

        let(:read_pref) do
          Mongo::ServerSelector.get(:mode => :primary)
        end

        let(:expected) do
          { }
        end

        it 'does not set the slave_ok flag' do
          expect(operation.send(:update_options_for_slave_ok, operation.send(:options), server)).to eq(expected)
        end
      end
    end
  end
end
