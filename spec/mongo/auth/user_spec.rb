require 'spec_helper'

describe Mongo::Auth::User do

  describe '#initialize' do

    let(:user) do
      described_class.new('testing', 'user', 'pass')
    end

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
end
