require 'spec_helper'

describe Mongo::ServerSelector do

  include_context 'server selector'

  describe '.get' do

    let(:selector) do
      described_class.get(:mode => name, :tag_sets => tag_sets)
    end

    context 'when the mode is primary' do

      let(:name) do
        :primary
      end

      it 'returns a read preference of class Primary' do
        expect(selector).to be_a(Mongo::ServerSelector::Primary)
      end
    end

    context 'when the mode is primary_preferred' do
      let(:name) do
        :primary_preferred
      end

      it 'returns a read preference of class PrimaryPreferred' do
        expect(selector).to be_a(Mongo::ServerSelector::PrimaryPreferred)
      end
    end

    context 'when the mode is secondary' do
      let(:name) do
        :secondary
      end

      it 'returns a read preference of class Secondary' do
        expect(selector).to be_a(Mongo::ServerSelector::Secondary)
      end
    end

    context 'when the mode is secondary_preferred' do
      let(:name) do
        :secondary_preferred
      end

      it 'returns a read preference of class SecondaryPreferred' do
        expect(selector).to be_a(Mongo::ServerSelector::SecondaryPreferred)
      end
    end

    context 'when the mode is nearest' do
      let(:name) do
        :nearest
      end

      it 'returns a read preference of class Nearest' do
        expect(selector).to be_a(Mongo::ServerSelector::Nearest)
      end
    end

    context 'when a mode is not provided' do
      let(:selector) { described_class.get }

      it 'returns a read preference of class Primary' do
        expect(selector).to be_a(Mongo::ServerSelector::Primary)
      end
    end

    context 'when tag sets are provided' do

      let(:selector) do
        described_class.get(:mode => :secondary, :tag_sets => tag_sets)
      end

      let(:tag_sets) do
        [{ 'test' => 'tag' }]
      end

      it 'sets tag sets on the read preference object' do
        expect(selector.tag_sets).to eq(tag_sets)
      end
    end

    context 'when server_selection_timeout is specified' do

      let(:selector) do
        described_class.get(:mode => :secondary, :server_selection_timeout => 1)
      end

      it 'sets server selection timeout on the read preference object' do
        expect(selector.server_selection_timeout).to eq(1)
      end
    end

    context 'when server_selection_timeout is not specified' do

      let(:selector) do
        described_class.get(:mode => :secondary)
      end

      it 'sets server selection timeout to the default' do
        expect(selector.server_selection_timeout).to eq(Mongo::ServerSelector::SERVER_SELECTION_TIMEOUT)
      end
    end

    context 'when local_threshold is specified' do

      let(:selector) do
        described_class.get(:mode => :secondary, :local_threshold => 0.010)
      end

      it 'sets local_threshold on the read preference object' do
        expect(selector.local_threshold).to eq(0.010)
      end
    end

    context 'when local_threshold is not specified' do

      let(:selector) do
        described_class.get(:mode => :secondary)
      end

      it 'sets local threshold to the default' do
        expect(selector.local_threshold).to eq(Mongo::ServerSelector::LOCAL_THRESHOLD)
      end
    end
  end

  describe "#select_server" do

    context 'when #select returns a list of nils' do

      let(:servers) do
        [ server(:primary) ]
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(false)
          allow(c).to receive(:sharded?).and_return(false)
          allow(c).to receive(:scan!).and_return(true)
        end
      end

      let(:read_pref) do
        described_class.get(mode: :primary, server_selection_timeout: 0.1).tap do |pref|
          allow(pref).to receive(:select).and_return([ nil, nil ])
        end
      end

      it 'raises a NoServerAvailable error' do
        expect do
          read_pref.select_server(cluster)
        end.to raise_exception(Mongo::Error::NoServerAvailable)
      end
    end
  end

  shared_context 'a ServerSelector' do

    context 'when cluster#servers is empty' do

      let(:servers) do
        []
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(single)
          allow(c).to receive(:sharded?).and_return(sharded)
          allow(c).to receive(:scan!).and_return(true)
        end
      end

      let(:read_pref) do
        described_class.get(mode: :primary, server_selection_timeout: 0.1)
      end

      it 'raises a NoServerAvailable error' do
        expect do
          read_pref.select_server(cluster)
        end.to raise_exception(Mongo::Error::NoServerAvailable)
      end
    end
  end

  context 'when the cluster has a Single topology' do

    let(:single) { true }
    let(:sharded) { false }

    it_behaves_like 'a ServerSelector'
  end

  context 'when the cluster has a ReplicaSet topology' do

    let(:single) { false }
    let(:sharded) { false }

    it_behaves_like 'a ServerSelector'
  end

  context 'when the cluster has a Sharded topology' do

    let(:single) { false }
    let(:sharded) { true }

    it_behaves_like 'a ServerSelector'
  end
end
