require 'spec_helper'

describe Mongo::ServerSelector do

  include_context 'server selector'

  describe '.get' do

    let(:selector) do
      described_class.get(:mode => name, :tag_sets => tag_sets)
    end

    context 'when a server selector object is passed' do

      let(:name) do
        :primary
      end

      it 'returns the object' do
        expect(described_class.get(selector)).to be(selector)
      end
    end

    context 'when the mode is primary' do

      let(:name) do
        :primary
      end

      it 'returns a read preference of class Primary' do
        expect(selector).to be_a(Mongo::ServerSelector::Primary)
      end

      context 'when the mode is a string' do

        let(:name) do
          'primary'
        end

        it 'returns a read preference of class Primary' do
          expect(selector).to be_a(Mongo::ServerSelector::Primary)
        end
      end
    end

    context 'when the mode is primary_preferred' do
      let(:name) do
        :primary_preferred
      end

      it 'returns a read preference of class PrimaryPreferred' do
        expect(selector).to be_a(Mongo::ServerSelector::PrimaryPreferred)
      end

      context 'when the mode is a string' do

        let(:name) do
          'primary_preferred'
        end

        it 'returns a read preference of class PrimaryPreferred' do
          expect(selector).to be_a(Mongo::ServerSelector::PrimaryPreferred)
        end
      end
    end

    context 'when the mode is secondary' do
      let(:name) do
        :secondary
      end

      it 'returns a read preference of class Secondary' do
        expect(selector).to be_a(Mongo::ServerSelector::Secondary)
      end

      context 'when the mode is a string' do

        let(:name) do
          'secondary'
        end

        it 'returns a read preference of class Secondary' do
          expect(selector).to be_a(Mongo::ServerSelector::Secondary)
        end
      end
    end

    context 'when the mode is secondary_preferred' do
      let(:name) do
        :secondary_preferred
      end

      it 'returns a read preference of class SecondaryPreferred' do
        expect(selector).to be_a(Mongo::ServerSelector::SecondaryPreferred)
      end

      context 'when the mode is a string' do

        let(:name) do
          'secondary_preferred'
        end

        it 'returns a read preference of class SecondaryPreferred' do
          expect(selector).to be_a(Mongo::ServerSelector::SecondaryPreferred)
        end
      end
    end

    context 'when the mode is nearest' do
      let(:name) do
        :nearest
      end

      it 'returns a read preference of class Nearest' do
        expect(selector).to be_a(Mongo::ServerSelector::Nearest)
      end

      context 'when the mode is a string' do

        let(:name) do
          'nearest'
        end

        it 'returns a read preference of class Nearest' do
          expect(selector).to be_a(Mongo::ServerSelector::Nearest)
        end
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
    skip_if_linting

    context 'when #select returns a list of nils' do

      let(:servers) do
        [ make_server(:primary) ]
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:summary)
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(false)
          allow(c).to receive(:sharded?).and_return(false)
          allow(c).to receive(:unknown?).and_return(false)
          allow(c).to receive(:scan!).and_return(true)
          allow(c).to receive(:options).and_return(server_selection_timeout: 0.1)
        end
      end

      let(:read_pref) do
        described_class.get(mode: :primary).tap do |pref|
          allow(pref).to receive(:select).and_return([ nil, nil ])
        end
      end

      it 'raises a NoServerAvailable error' do
        expect do
          read_pref.select_server(cluster)
        end.to raise_exception(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when the cluster has a server_selection_timeout set' do

      let(:servers) do
        [ make_server(:secondary), make_server(:primary) ]
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:summary)
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(false)
          allow(c).to receive(:sharded?).and_return(false)
          allow(c).to receive(:unknown?).and_return(false)
          allow(c).to receive(:scan!).and_return(true)
          allow(c).to receive(:options).and_return(server_selection_timeout: 0)
        end
      end

      let(:read_pref) do
        described_class.get(mode: :nearest)
      end

      it 'uses the server_selection_timeout of the cluster' do
        expect{
          read_pref.select_server(cluster)
        }.to raise_exception(Mongo::Error::NoServerAvailable)
      end
    end

    context 'when the cluster has a local_threshold set' do

      let(:near_server) do
        make_server(:secondary).tap do |s|
          allow(s).to receive(:connectable?).and_return(true)
          allow(s).to receive(:average_round_trip_time).and_return(100)
          allow(s).to receive(:check_driver_support!).and_return(true)
        end
      end

      let(:far_server) do
        make_server(:secondary).tap do |s|
          allow(s).to receive(:connectable?).and_return(true)
          allow(s).to receive(:average_round_trip_time).and_return(200)
          allow(s).to receive(:check_driver_support!).and_return(true)
        end
      end

      let(:servers) do
        [ near_server, far_server ]
      end

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:summary)
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(false)
          allow(c).to receive(:sharded?).and_return(false)
          allow(c).to receive(:unknown?).and_return(false)
          allow(c).to receive(:scan!).and_return(true)
          allow(c).to receive(:options).and_return(local_threshold: 0.050)
        end
      end

      let(:read_pref) do
        described_class.get(mode: :nearest)
      end

      it 'uses the local_threshold of the cluster' do
        expect(topology).to receive(:compatible?).and_return(true)
        expect(topology).to receive(:single?).and_return(false)
        expect(read_pref.select_server(cluster)).to eq(near_server)
      end
    end

    context 'when topology is incompatible' do
      let(:server) { make_server(:primary) }

      let(:cluster) do
        double('cluster').tap do |c|
          allow(c).to receive(:summary)
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:servers).and_return([server])
          allow(c).to receive(:single?).and_return(false)
          allow(c).to receive(:sharded?).and_return(false)
          allow(c).to receive(:unknown?).and_return(false)
          allow(c).to receive(:scan!).and_return(true)
          allow(c).to receive(:options).and_return(local_threshold: 0.050)
        end
      end

      let(:compatibility_error) do
        Mongo::Error::UnsupportedFeatures.new('Test UnsupportedFeatures')
      end

      let(:selector) { described_class.get(mode: :primary) }

      it 'raises Error::UnsupportedFeatures' do
        expect(topology).to receive(:compatible?).and_return(false)
        expect(topology).to receive(:compatibility_error).and_return(compatibility_error)
        expect do
          selector.select_server(cluster)
        end.to raise_error(Mongo::Error::UnsupportedFeatures, 'Test UnsupportedFeatures')
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
          allow(c).to receive(:summary)
          allow(c).to receive(:topology).and_return(topology)
          allow(c).to receive(:servers).and_return(servers)
          allow(c).to receive(:single?).and_return(single)
          allow(c).to receive(:sharded?).and_return(sharded)
          allow(c).to receive(:unknown?).and_return(false)
          allow(c).to receive(:scan!).and_return(true)
          allow(c).to receive(:options).and_return(server_selection_timeout: 0.1)
        end
      end

      let(:read_pref) do
        described_class.get(mode: :primary)
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

  describe '#inspect' do

    let(:options) do
      {}
    end

    let(:read_pref) do
      described_class.get({ mode: mode }.merge(options))
    end

    context 'when the mode is primary' do

      let(:mode) do
        :primary
      end

      it 'includes the mode in the inspect string' do
        expect(read_pref.inspect).to match(/#{mode.to_s}/i)
      end
    end

    context 'when there are tag sets' do

      let(:mode) do
        :secondary
      end

      let(:options) do
        { tag_sets: [{ 'data_center' => 'nyc' }] }
      end

      it 'includes the tag sets in the inspect string' do
        expect(read_pref.inspect).to include(options[:tag_sets].inspect)
      end
    end

    context 'when there is a max staleness set' do

      let(:mode) do
        :secondary
      end

      let(:options) do
        { max_staleness: 123 }
      end

      it 'includes staleness in the inspect string' do
        expect(read_pref.inspect).to match(/max_staleness/i)
        expect(read_pref.inspect).to match(/123/)
      end
    end
  end

  describe '#filter_stale_servers' do
    skip_if_linting

    include_context 'server selector'
    let(:name) do
      :secondary
    end
    let(:selector) { Mongo::ServerSelector::Secondary.new(
      mode: name, max_staleness: max_staleness) }

    def make_server_with_staleness(last_write_date)
      make_server(:secondary).tap do |server|
        allow(server.description.features).to receive(:max_staleness_enabled?).and_return(true)
        allow(server).to receive(:last_scan).and_return(Time.now)
        allow(server).to receive(:last_write_date).and_return(last_write_date)
      end
    end

    shared_context 'staleness filter' do
      let(:servers) do
        [recent_server, stale_server]
      end

      context 'when max staleness is not set' do
        let(:max_staleness) { nil }

        it 'filters correctly' do
          result = selector.send(:filter_stale_servers, servers, primary)
          expect(result).to eq([recent_server, stale_server])
        end
      end

      context 'when max staleness is set' do
        let(:max_staleness) { 100 }

        it 'filters correctly' do
          result = selector.send(:filter_stale_servers, servers, primary)
          expect(result).to eq([recent_server])
        end
      end
    end

    context 'primary is given' do
      let(:primary) do
        make_server(:primary).tap do |server|
          allow(server).to receive(:last_scan).and_return(Time.now)
          allow(server).to receive(:last_write_date).and_return(Time.now-100)
        end
      end

      # staleness is relative to primary, which itself is 100 seconds stale
      let(:recent_server) { make_server_with_staleness(Time.now-110) }
      let(:stale_server) { make_server_with_staleness(Time.now-210) }

      it_behaves_like 'staleness filter'
    end

    context 'primary is not given' do
      let(:primary) { nil }

      let(:recent_server) { make_server_with_staleness(Time.now-1) }
      let(:stale_server) { make_server_with_staleness(Time.now-110) }

      it_behaves_like 'staleness filter'
    end
  end
end
