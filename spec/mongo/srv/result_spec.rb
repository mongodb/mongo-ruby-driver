# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Srv::Result do
  let(:result) do
    described_class.new('bar.com')
  end

  describe '#add_record' do
    context 'when incoming hostname is in mixed case' do
      let(:record) do
        double('record').tap do |record|
          allow(record).to receive(:target).and_return('FOO.bar.COM')
          allow(record).to receive(:port).and_return(42)
          allow(record).to receive(:ttl).and_return(1)
        end
      end

      it 'stores hostname in lower case' do
        result.add_record(record)
        expect(result.address_strs).to eq(['foo.bar.com:42'])
      end
    end

    exampleSrvName = ['i-love-rb', 'i-love-rb.mongodb', 'i-love-ruby.mongodb.io'];
    exampleHostName = [
      'rb-00.i-love-rb',
      'rb-00.i-love-rb.mongodb',
      'i-love-ruby-00.mongodb.io'
    ];
    exampleHostNameThatDoNotMatchParent = [
      'rb-00.i-love-rb-a-little',
      'rb-00.i-love-rb-a-little.mongodb',
      'i-love-ruby-00.evil-mongodb.io'
    ];

    (0..2).each do |i|
      context "when srvName has #{i+1} part#{i != 0 ? 's' : ''}" do
        let(:srv_name) { exampleSrvName[i] }
        let(:host_name) { exampleHostName[i] }
        let(:mismatched_host_name) { exampleHostNameThatDoNotMatchParent[i] }
        
        context 'when address does not match parent domain' do
          it 'raises MismatchedDomain error' do
            record = double('record').tap do |record|
              allow(record).to receive(:target).and_return(mismatched_host_name)
              allow(record).to receive(:port).and_return(42)
              allow(record).to receive(:ttl).and_return(1)
            end

            expect {
              result = described_class.new(srv_name)
              result.add_record(record)
            }.to raise_error(Mongo::Error::MismatchedDomain)
          end
        end
        
        context 'when address matches parent domain' do
          it 'adds the record' do
            record = double('record').tap do |record|
              allow(record).to receive(:target).and_return(host_name)
              allow(record).to receive(:port).and_return(42)
              allow(record).to receive(:ttl).and_return(1)
            end

            result = described_class.new(srv_name)
            result.add_record(record)

            expect(result.address_strs).to eq([host_name + ':42'])
          end
        end

        if i < 2
          context 'when the address is less than 3 parts' do
            it 'does not accept address if it does not contain an extra domain level' do
              record = double('record').tap do |record|
                allow(record).to receive(:target).and_return(srv_name)
                allow(record).to receive(:port).and_return(42)
                allow(record).to receive(:ttl).and_return(1)
              end

              expect {
                result = described_class.new(srv_name)
                result.add_record(record)
              }.to raise_error(Mongo::Error::MismatchedDomain)
            end
          end
        end
      end
    end
  end

  describe '#normalize_hostname' do
    let(:actual) do
      result.send(:normalize_hostname, hostname)
    end

    context 'when hostname is in mixed case' do
      let(:hostname) { 'FOO.bar.COM' }

      it 'converts to lower case' do
        expect(actual).to eq('foo.bar.com')
      end
    end

    context 'when hostname has one trailing dot' do
      let(:hostname) { 'foo.' }

      it 'removes the trailing dot' do
        expect(actual).to eq('foo')
      end
    end

    context 'when hostname has multiple trailing dots' do
      let(:hostname) { 'foo..' }

      it 'returns hostname unchanged' do
        expect(actual).to eq('foo..')
      end
    end
  end
end
