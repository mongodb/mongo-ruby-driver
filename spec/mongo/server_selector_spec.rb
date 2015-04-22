require 'spec_helper'

describe Mongo::ServerSelector do

  include_context 'server selector'

  describe '.get' do

    let(:read_pref) do
      described_class.get({ :mode => name })
    end

    let(:name) { :secondary }
    let(:tag_sets) { [{ 'test' => 'tag' }] }

    context 'when the mode is primary' do
      let(:name) { :primary }

      it 'returns a read preference of class Primary' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Primary)
      end
    end

    context 'when the mode is primary_preferred' do
      let(:name) { :primary_preferred }

      it 'returns a read preference of class PrimaryPreferred' do
        expect(read_pref).to be_a(Mongo::ServerSelector::PrimaryPreferred)
      end
    end

    context 'when the mode is secondary' do
      let(:name) { :secondary }

      it 'returns a read preference of class Secondary' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Secondary)
      end
    end

    context 'when the mode is secondary_preferred' do
      let(:name) { :secondary_preferred }

      it 'returns a read preference of class SecondaryPreferred' do
        expect(read_pref).to be_a(Mongo::ServerSelector::SecondaryPreferred)
      end
    end

    context 'when the mode is nearest' do
      let(:name) { :nearest }

      it 'returns a read preference of class Nearest' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Nearest)
      end
    end

    context 'when a mode is not provided' do
      let(:read_pref) { described_class.get }

      it 'returns a read preference of class Primary' do
        expect(read_pref).to be_a(Mongo::ServerSelector::Primary)
      end
    end

    context 'when tag sets provided' do
      let(:read_pref) { described_class.get(:mode => name, :tag_sets => tag_sets) }

      it 'sets tag sets on the read preference object' do
        expect(read_pref.tag_sets).to eq(tag_sets)
      end
    end
  end

  describe "#select_server" do

    context 'when #select returns a list of nils' do

      let(:servers) { [ server(:primary) ] }

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(false)
          allow(c).to receive(:sharded?).and_return(false)
          allow(c).to receive(:scan!).and_return(true)
        end
      end

      let(:read_pref) do
        described_class.get({ mode: :primary }, server_selection_timeout: 1).tap do |pref|
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

      let(:servers) { [] }

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(single)
          allow(c).to receive(:sharded?).and_return(sharded)
          allow(c).to receive(:scan!).and_return(true)
        end
      end

      let(:read_pref) do
        described_class.get({ mode: :primary }, server_selection_timeout: 1)
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
