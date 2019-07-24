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
end
