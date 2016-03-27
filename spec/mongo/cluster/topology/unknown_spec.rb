require 'spec_helper'

describe Mongo::Cluster::Topology::Unknown do

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:topology) do
    described_class.new({}, monitoring)
  end

  describe '.servers' do

    let(:servers) do
      topology.servers([ double('mongos'), double('standalone') ])
    end

    it 'returns an empty array' do
      expect(servers).to eq([ ])
    end
  end

  describe '.replica_set?' do

    it 'returns false' do
      expect(topology).to_not be_replica_set
    end
  end

  describe '.sharded?' do

    it 'returns false' do
      expect(topology).not_to be_sharded
    end
  end

  describe '.single?' do

    it 'returns false' do
      expect(topology).not_to be_single
    end
  end

  describe '.unknown?' do

    it 'returns true' do
      expect(topology.unknown?).to be(true)
    end
  end

  describe '#has_readable_servers?' do

    it 'returns false' do
      expect(topology).to_not have_readable_server(nil, nil)
    end
  end

  describe '#has_writable_servers?' do

    it 'returns false' do
      expect(topology).to_not have_writable_server(nil)
    end
  end

  describe '#add_hosts?' do

    context 'when the description is from an unknown server' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:unknown?).and_return(true)
        end
      end

      it 'returns false' do
        expect(topology.add_hosts?(description, [])).to be(false)
      end
    end

    context 'when the description is from a ghost server' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:unknown?).and_return(false)
          allow(d).to receive(:ghost?).and_return(true)
        end
      end

      it 'returns false' do
        expect(topology.add_hosts?(description, [])).to be(false)
      end
    end

    context 'when the description is not from an unknown or ghost' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:unknown?).and_return(false)
          allow(d).to receive(:ghost?).and_return(false)
        end
      end

      it 'returns true' do
        expect(topology.add_hosts?(description, [])).to be(true)
      end
    end
  end

  describe '#remove_hosts?' do

    context 'when the description is from a standalone' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:standalone?).and_return(true)
        end
      end

      it 'returns true' do
        expect(topology.remove_hosts?(description)).to be(true)
      end
    end

    context 'when the description is not from a standalone' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:standalone?).and_return(false)
        end
      end

      it 'returns true' do
        expect(topology.remove_hosts?(description)).to be(false)
      end
    end
  end

  describe '#remove_server?' do

     context 'when the description is from a standalone' do

       let(:description) do
         double('description').tap do |d|
           allow(d).to receive(:standalone?).and_return(true)
           allow(d).to receive(:is_server?).and_return(true)
         end
       end

       context 'when the description is from the server in question' do

         it 'returns true' do
           expect(topology.remove_server?(description, double('server'))).to be(true)
         end
       end

       context 'when the description is not from the server in question' do

         let(:description) do
           double('description').tap do |d|
             allow(d).to receive(:standalone?).and_return(true)
             allow(d).to receive(:is_server?).and_return(false)
           end
         end

         it 'returns false' do
           expect(topology.remove_server?(description, double('server'))).to be(false)
         end
       end
     end

    context 'when the description is not from a standalone' do

      let(:description) do
        double('description').tap do |d|
          allow(d).to receive(:standalone?).and_return(false)
        end
      end

      it 'returns false' do
        expect(topology.remove_server?(description, double('server'))).to be(false)
      end
    end
  end
end
