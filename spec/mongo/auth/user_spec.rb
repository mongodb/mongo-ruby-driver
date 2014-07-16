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
        expect(user.mechanism).to eq(:mongodb_cr)
      end
    end
  end

  describe '#gssapi_service_name' do

    context 'when the option is provided' do

      let(:options) do
        { database: 'testing', user: 'user', password: 'pass', gssapi_service_name: 'test' }
      end

      let(:user) do
        described_class.new(options)
      end

      it 'returns the option' do
        expect(user.gssapi_service_name).to eq('test')
      end
    end

    context 'when no option is provided' do

      let(:user) do
        described_class.new(options)
      end

      it 'returns the default' do
        expect(user.gssapi_service_name).to eq('mongodb')
      end
    end
  end

  describe '#canonicalize_host_name' do

    context 'when the option is provided' do

      let(:options) do
        { database: 'testing', user: 'user', password: 'pass', canonicalize_host_name: true }
      end

      let(:user) do
        described_class.new(options)
      end

      it 'returns the option' do
        expect(user.canonicalize_host_name).to be true
      end
    end

    context 'when no option is provided' do

      let(:user) do
        described_class.new(options)
      end

      it 'returns the default' do
        expect(user.canonicalize_host_name).to be false
      end
    end
  end
end
