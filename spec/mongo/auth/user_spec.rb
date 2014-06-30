require 'spec_helper'

describe Mongo::Auth::User do

  let(:user) do
    described_class.new('testing', 'user', 'pass')
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
end
