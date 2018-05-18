require 'spec_helper'

describe Mongo::Auth::User do

  let(:options) do
    { database: 'testing', user: 'user', password: 'pass' }
  end

  let(:user) do
    described_class.new(options)
  end

  describe '#auth_key' do

    let(:nonce) do

    end

    let(:expected) do
      Digest::MD5.hexdigest("#{nonce}#{user.name}#{user.hashed_password}")
    end

    it 'returns the users authentication key' do
      expect(user.auth_key(nonce)).to eq(expected)
    end
  end

  describe '#encoded_name' do

    context 'when the user name contains an =' do

      let(:options) do
        { user: 'user=' }
      end

      it 'escapes the = character to =3D' do
        expect(user.encoded_name).to eq('user=3D')
      end

      it 'returns a UTF-8 string' do
        expect(user.encoded_name.encoding.name).to eq('UTF-8')
      end
    end

    context 'when the user name contains a ,' do

      let(:options) do
        { user: 'user,' }
      end

      it 'escapes the , character to =2C' do
        expect(user.encoded_name).to eq('user=2C')
      end

      it 'returns a UTF-8 string' do
        expect(user.encoded_name.encoding.name).to eq('UTF-8')
      end
    end

    context 'when the user name contains no special characters' do

      it 'does not alter the user name' do
        expect(user.name).to eq('user')
      end

      it 'returns a UTF-8 string' do
        expect(user.encoded_name.encoding.name).to eq('UTF-8')
      end
    end
  end

  describe '#initialize' do

    it 'sets the database' do
      expect(user.database).to eq('testing')
    end

    it 'sets the name' do
      expect(user.name).to eq('user')
    end

    it 'sets the password' do
      expect(user.password).to eq('pass')
    end
  end

  describe '#hashed_password' do

    let(:expected) do
      Digest::MD5.hexdigest("user:mongo:pass")
    end

    it 'returns the hashed password' do
      expect(user.hashed_password).to eq(expected)
    end
  end

  describe '#mechanism' do

    context 'when the option is provided' do

      let(:options) do
        { database: 'testing', user: 'user', password: 'pass', auth_mech: :plain }
      end

      let(:user) do
        described_class.new(options)
      end

      it 'returns the option' do
        expect(user.mechanism).to eq(:plain)
      end
    end

    context 'when no option is provided' do

      let(:user) do
        described_class.new(options)
      end

      it 'returns the default' do
        expect(user.mechanism).to be_nil
      end
    end
  end

  describe '#auth_mech_properties' do

    context 'when the option is provided' do

      let(:auth_mech_properties) do
        { service_name: 'test',
          service_realm: 'test',
          canonicalize_host_name: true }
      end

      let(:options) do
        { database: 'testing', user: 'user', password: 'pass', auth_mech_properties: auth_mech_properties }
      end

      let(:user) do
        described_class.new(options)
      end

      it 'returns the option' do
        expect(user.auth_mech_properties).to eq(auth_mech_properties)
      end
    end

    context 'when no option is provided' do

      let(:user) do
        described_class.new(options)
      end

      it 'returns an empty hash' do
        expect(user.auth_mech_properties).to eq({})
      end
    end
  end

  describe '#roles' do

    context 'when roles are provided' do

      let(:roles) do
        [ Mongo::Auth::Roles::ROOT ]
      end

      let(:user) do
        described_class.new(roles: roles)
      end

      it 'returns the roles' do
        expect(user.roles).to eq(roles)
      end
    end

    context 'when no roles are provided' do

      let(:user) do
        described_class.new({})
      end

      it 'returns an empty array' do
        expect(user.roles).to be_empty
      end
    end
  end
end
