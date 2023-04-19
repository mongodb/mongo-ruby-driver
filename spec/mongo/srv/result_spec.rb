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
