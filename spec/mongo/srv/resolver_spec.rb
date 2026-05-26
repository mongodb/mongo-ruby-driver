# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Srv::Resolver do
  describe '#get_records' do
    let(:hostname) { 'foo.example.com' }
    let(:srv_query) { '_mongodb._tcp.' + hostname }

    let(:mismatched_record) do
      double('record').tap do |record|
        allow(record).to receive(:target).and_return('evil.attacker.tld')
        allow(record).to receive(:port).and_return(27_017)
        allow(record).to receive(:ttl).and_return(1)
      end
    end

    let(:dns_resolver) do
      instance_double(Resolv::DNS).tap do |dns|
        allow(dns).to receive(:timeouts=)
      end
    end

    before do
      allow(Resolv::DNS).to receive(:new).and_return(dns_resolver)
    end

    context 'when raise_on_invalid is false and a mismatched-domain record is returned' do
      let(:resolver) { described_class.new(raise_on_invalid: false) }

      before do
        allow(dns_resolver).to receive(:getresources)
          .with(srv_query, Resolv::DNS::Resource::IN::SRV)
          .and_return([ mismatched_record ])
      end

      it 'does not raise and logs a warning' do
        expect(resolver).to receive(:log_warn).at_least(:once)
        expect { resolver.get_records(hostname) }.not_to raise_error
      end
    end

    context 'when raise_on_invalid is false and no records are returned' do
      let(:resolver) { described_class.new(raise_on_invalid: false) }

      before do
        allow(dns_resolver).to receive(:getresources)
          .with(srv_query, Resolv::DNS::Resource::IN::SRV)
          .and_return([])
      end

      it 'does not raise NoSRVRecords' do
        allow(resolver).to receive(:log_warn)
        expect { resolver.get_records(hostname) }.not_to raise_error
      end
    end

    context 'when raise_on_invalid is true (default) and a mismatched-domain record is returned' do
      let(:resolver) { described_class.new }

      before do
        allow(dns_resolver).to receive(:getresources)
          .with(srv_query, Resolv::DNS::Resource::IN::SRV)
          .and_return([ mismatched_record ])
      end

      it 'raises MismatchedDomain' do
        expect { resolver.get_records(hostname) }
          .to raise_error(Mongo::Error::MismatchedDomain)
      end
    end

    context 'when raise_on_invalid is true (default) and no records are returned' do
      let(:resolver) { described_class.new }

      before do
        allow(dns_resolver).to receive(:getresources)
          .with(srv_query, Resolv::DNS::Resource::IN::SRV)
          .and_return([])
      end

      it 'raises NoSRVRecords' do
        expect { resolver.get_records(hostname) }
          .to raise_error(Mongo::Error::NoSRVRecords)
      end
    end
  end
end
