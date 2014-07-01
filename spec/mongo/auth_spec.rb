require 'spec_helper'

describe Mongo::Auth do

  describe '#get' do

    context 'when nil is provided' do

      let(:cr) do
        described_class.get(nil)
      end

      it 'returns CR' do
        expect(cr).to eq(Mongo::Auth::CR)
      end
    end

    context 'when mongodb_cr is provided' do

      let(:cr) do
        described_class.get(:mongodb_cr)
      end

      it 'returns CR' do
        expect(cr).to eq(Mongo::Auth::CR)
      end
    end

    context 'when mongodb_x509 is provided' do

      let(:x509) do
        described_class.get(:mongodb_x509)
      end

      it 'returns X509' do
        expect(x509).to eq(Mongo::Auth::X509)
      end
    end

    context 'when plain is provided' do

      let(:ldap) do
        described_class.get(:plain)
      end

      it 'returns LDAP' do
        expect(ldap).to eq(Mongo::Auth::LDAP)
      end
    end

    context 'when an invalid mechanism is provided' do

      it 'raises an error' do
        expect {
          described_class.get(:test)
        }.to raise_error(Mongo::Auth::InvalidMechanism)
      end
    end
  end
end
