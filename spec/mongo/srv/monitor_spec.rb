require 'lite_spec_helper'

describe Mongo::SRV::Monitor do
  describe '#scan!' do
    let(:hostname) do
      'test1.test.build.10gen.cc.'
    end

    let(:hosts) do
      [
        'localhost.test.build.10gen.cc.:27017',
        'localhost.test.build.10gen.cc.:27018',
      ]
    end

    let(:records) do
      double('records').tap do |records|
        allow(records).to receive(:hostname).and_return(hostname)
        allow(records).to receive(:hosts).and_return(hosts)
        allow(records).to receive(:empty?).and_return(false)
        allow(records).to receive(:min_ttl).and_return(nil)
      end
    end


    let(:cluster) do
      Mongo::Cluster.new(records.hosts, Mongo::Monitoring.new, { monitoring_io: false })
    end

    let(:monitoring) do
      described_class.new(cluster, resolver, records)
    end

    before do
      monitoring.scan!
    end

    context 'when a new DNS record is added' do
      let(:new_hosts) do
        hosts + ['test1.test.build.10gen.cc.:27019']
      end

      let(:new_records) do
        double('records').tap do |records|
          allow(records).to receive(:hostname).and_return(hostname)
          allow(records).to receive(:hosts).and_return(new_hosts)
          allow(records).to receive(:empty?).and_return(false)
          allow(records).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_records)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when a DNS record is removed' do
      let(:new_hosts) do
        hosts - ['test1.test.build.10gen.cc.:27018']
      end

      let(:new_records) do
        double('records').tap do |records|
          allow(records).to receive(:hostname).and_return(hostname)
          allow(records).to receive(:hosts).and_return(new_hosts)
          allow(records).to receive(:empty?).and_return(false)
          allow(records).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_records)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when a single DNS record is replaced' do
      let(:new_hosts) do
        hosts - ['test1.test.build.10gen.cc.:27018'] +  ['test1.test.build.10gen.cc.:27019']
      end

      let(:new_records) do
        double('records').tap do |records|
          allow(records).to receive(:hostname).and_return(hostname)
          allow(records).to receive(:hosts).and_return(new_hosts)
          allow(records).to receive(:empty?).and_return(false)
          allow(records).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_records)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when all DNS records are replaced with a single record' do
      let(:new_hosts) do
        ['test1.test.build.10gen.cc.:27019']
      end

      let(:new_records) do
        double('records').tap do |records|
          allow(records).to receive(:hostname).and_return(hostname)
          allow(records).to receive(:hosts).and_return(new_hosts)
          allow(records).to receive(:empty?).and_return(false)
          allow(records).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_records)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when all DNS records are replaced with multiple records' do
      let(:new_hosts) do
        [
          'test1.test.build.10gen.cc.:27019',
          'test1.test.build.10gen.cc.:27020',
        ]
      end

      let(:new_records) do
        double('records').tap do |records|
          allow(records).to receive(:hostname).and_return(hostname)
          allow(records).to receive(:hosts).and_return(new_hosts)
          allow(records).to receive(:empty?).and_return(false)
          allow(records).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_records)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when the DNS lookup times out' do
      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_raise(Resolv::ResolvTimeout)
        end
      end

      it 'does not add or remove any hosts from the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(hosts.sort)
      end
    end

    context 'when the DNS lookup is unable to resolve the hostname' do
     let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_raise(Resolv::ResolvError)
        end
      end

      it 'does not add or remove any hosts from the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(hosts.sort)
      end
    end

    context 'when no DNS records are returned' do
      let(:new_records) do
        double('records').tap do |records|
          allow(records).to receive(:hostname).and_return(hostname)
          allow(records).to receive(:hosts).and_return([])
          allow(records).to receive(:empty?).and_return(true)
          allow(records).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_records)
        end
      end

      it 'does not add or remove any hosts from the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(hosts.sort)
      end
    end
  end
end