require 'spec_helper'

describe Mongo::Operation::ReadPreferrable do

  let(:selector) do
    { name: 'test' }
  end

  let(:options) do
    {}
  end

  let(:mongos) do
    false
  end

  let(:slave_ok) do
    false
  end

  let(:read_preferrable) do
    Class.new do
      include Mongo::Operation::ReadPreferrable
    end.new.tap do |rp|
      allow(rp).to receive(:read).and_return(read_pref)
      allow(rp).to receive(:selector).and_return(selector)
      allow(rp).to receive(:options).and_return(options)
    end
  end

  let(:context) do
    double('context').tap do |c|
      allow(c).to receive(:slave_ok?).and_return(slave_ok)
      allow(c).to receive(:mongos?).and_return(mongos)
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
      expect(read_preferrable.send(:update_selector, context)).to eq(expected)
    end

    context 'when the selector already has $query in it' do

      let(:selector) do
        { :$query => { :name => 'test' },
          :$orderby => { :name => -1 } }
      end

      let(:expected) do
        selector.merge(:$readPreference => read_pref.to_mongos)
      end

      it 'returns a special selector' do
        expect(read_preferrable.send(:update_selector, context)).to eq(expected)
      end
    end
  end

  shared_context 'not a selector updater' do

    let(:read_pref) do
      Mongo::ServerSelector.get(:mode => mode)
    end

    it 'returns a special selector' do
      expect(read_preferrable.send(:update_selector, context)).to eq(selector)
    end
  end

  context 'when the server is a mongos' do

    let(:mongos) do
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

    let(:mode) do
      :secondary_preferred
    end

    it_behaves_like 'not a selector updater'
  end

  context 'when the server context requires the slaveOk bit to be set' do

    let(:read_pref) do
      Mongo::ServerSelector.get(:mode => :secondary)
    end

    let(:expected) do
      { :flags => [ :slave_ok ] }
    end

    let(:slave_ok) do
      true
    end

    it 'sets the slave_ok flag' do
      expect(read_preferrable.send(:update_options, context)).to eq(expected)
    end
  end

  context 'when the server is not a mongos' do

    context 'when the read preference requires the slaveOk bit to be set' do

      let(:read_pref) do
        Mongo::ServerSelector.get(:mode => :secondary)
      end

      let(:expected) do
        { :flags => [ :slave_ok ] }
      end

      it 'sets the slave_ok flag' do
        expect(read_preferrable.send(:update_options, context)).to eq(expected)
      end
    end

    context 'when the read preference does not require the slaveOk bit to be set' do

      let(:read_pref) do
        Mongo::ServerSelector.get(:mode => :primary)
      end

      let(:expected) do
        { }
      end

      it 'sets the slave_ok flag' do
        expect(read_preferrable.send(:update_options, context)).to eq(expected)
      end
    end
  end
end
