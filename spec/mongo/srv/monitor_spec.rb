# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Srv::Monitor do
  describe '#scan!' do
    let(:hostname) do
      'test1.test.build.10gen.cc'
    end

    let(:hosts) do
      [
        'localhost.test.build.10gen.cc:27017',
        'localhost.test.build.10gen.cc:27018',
      ]
    end

    let(:result) do
      double('result').tap do |result|
        allow(result).to receive(:hostname).and_return(hostname)
        allow(result).to receive(:address_strs).and_return(hosts)
        allow(result).to receive(:empty?).and_return(false)
        allow(result).to receive(:min_ttl).and_return(nil)
      end
    end

    let(:uri_resolver) do
      double('uri resolver').tap do |resolver|
        expect(resolver).to receive(:get_records).and_return(result)
      end
    end

    let(:srv_uri) do
      Mongo::URI.get("mongodb+srv://this.is.not.used")
    end

    let(:cluster) do
      Mongo::Cluster.new(hosts, Mongo::Monitoring.new, monitoring_io: false)
    end

    let(:monitor) do
      described_class.new(cluster, srv_uri: srv_uri)
    end

    before do
      # monitor instantiation triggers cluster instantiation which
      # performs real SRV lookups for the hostname.
      # The next lookup (the one performed when cluster is already set up)
      # is using our doubles.
      RSpec::Mocks.with_temporary_scope do
        allow(uri_resolver).to receive(:get_txt_options_string)
        expect(Mongo::Srv::Resolver).to receive(:new).ordered.and_return(uri_resolver)
        allow(resolver).to receive(:get_txt_options_string)
        expect(Mongo::Srv::Resolver).to receive(:new).ordered.and_return(resolver)
        monitor.send(:scan!)
      end
    end

    context 'when a new DNS record is added' do
      let(:new_hosts) do
        hosts + ['localhost.test.build.10gen.cc:27019']
      end

      let(:new_result) do
        double('result').tap do |result|
          allow(result).to receive(:hostname).and_return(hostname)
          allow(result).to receive(:address_strs).and_return(new_hosts)
          allow(result).to receive(:empty?).and_return(false)
          allow(result).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('monitor resolver').tap do |resolver|
          expect(resolver).to receive(:get_records).and_return(new_result)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.servers_list.map(&:address).map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when a DNS record is removed' do
      let(:new_hosts) do
        hosts - ['test1.test.build.10gen.cc:27018']
      end

      let(:new_result) do
        double('result').tap do |result|
          allow(result).to receive(:hostname).and_return(hostname)
          allow(result).to receive(:address_strs).and_return(new_hosts)
          allow(result).to receive(:empty?).and_return(false)
          allow(result).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_result)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when a single DNS record is replaced' do
      let(:new_hosts) do
        hosts - ['test1.test.build.10gen.cc:27018'] +  ['test1.test.build.10gen.cc:27019']
      end

      let(:new_result) do
        double('result').tap do |result|
          allow(result).to receive(:hostname).and_return(hostname)
          allow(result).to receive(:address_strs).and_return(new_hosts)
          allow(result).to receive(:empty?).and_return(false)
          allow(result).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_result)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when all DNS result are replaced with a single record' do
      let(:new_hosts) do
        ['test1.test.build.10gen.cc:27019']
      end

      let(:new_result) do
        double('result').tap do |result|
          allow(result).to receive(:hostname).and_return(hostname)
          allow(result).to receive(:address_strs).and_return(new_hosts)
          allow(result).to receive(:empty?).and_return(false)
          allow(result).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          expect(resolver).to receive(:get_records).and_return(new_result)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when all DNS result are replaced with multiple result' do
      let(:new_hosts) do
        [
          'test1.test.build.10gen.cc:27019',
          'test1.test.build.10gen.cc:27020',
        ]
      end

      let(:new_result) do
        double('result').tap do |result|
          allow(result).to receive(:hostname).and_return(hostname)
          allow(result).to receive(:address_strs).and_return(new_hosts)
          allow(result).to receive(:empty?).and_return(false)
          allow(result).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_result)
        end
      end

      it 'adds the new host to the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(new_hosts.sort)
      end
    end

    context 'when the DNS lookup times out' do
      let(:resolver) do
        double('resolver').tap do |resolver|
          expect(resolver).to receive(:get_records).and_raise(Resolv::ResolvTimeout)
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

    context 'when no DNS result are returned' do
      let(:new_result) do
        double('result').tap do |result|
          allow(result).to receive(:hostname).and_return(hostname)
          allow(result).to receive(:address_strs).and_return([])
          allow(result).to receive(:empty?).and_return(true)
          allow(result).to receive(:min_ttl).and_return(nil)
        end
      end

      let(:resolver) do
        double('resolver').tap do |resolver|
          allow(resolver).to receive(:get_records).and_return(new_result)
        end
      end

      it 'does not add or remove any hosts from the cluster' do
        expect(cluster.addresses.map(&:to_s).sort).to eq(hosts.sort)
      end
    end
  end
end
