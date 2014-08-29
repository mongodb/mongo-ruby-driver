require 'spec_helper'

describe Mongo::Auth do

  describe '#get' do

    context 'when a mongodb_cr user is provided' do

      let(:user) do
        Mongo::Auth::User.new(auth_mech: :mongodb_cr)
      end

      let(:cr) do
        described_class.get(user)
      end

      it 'returns CR' do
        expect(cr).to be_a(Mongo::Auth::CR)
      end
    end

    context 'when a mongodb_x509 user is provided' do

      let(:user) do
        Mongo::Auth::User.new(auth_mech: :mongodb_x509)
      end

      let(:x509) do
        described_class.get(user)
      end

      it 'returns X509' do
        expect(x509).to be_a(Mongo::Auth::X509)
      end
    end

    context 'when a plain user is provided' do

      let(:user) do
        Mongo::Auth::User.new(auth_mech: :plain)
      end

      let(:ldap) do
        described_class.get(user)
      end

      it 'returns LDAP' do
        expect(ldap).to be_a(Mongo::Auth::LDAP)
      end
    end

    context 'when an invalid mechanism is provided' do

      let(:user) do
        Mongo::Auth::User.new(auth_mech: :nothing)
      end

      it 'raises an error' do
        expect {
          described_class.get(user)
        }.to raise_error(Mongo::Auth::InvalidMechanism)
      end
    end
  end
end
