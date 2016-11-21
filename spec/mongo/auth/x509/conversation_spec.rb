require 'spec_helper'

describe Mongo::Auth::X509::Conversation do

  let(:user) do
    Mongo::Auth::User.new(
      database: Mongo::Database::ADMIN,
      user: 'user',
    )
  end

  let(:conversation) do
    described_class.new(user)
  end

  describe '#start' do

    let(:query) do
      conversation.start
    end

    let(:selector) do
      query.selector
    end

    it 'sets username' do
      expect(selector[:user]).to eq('user')
    end

    it 'sets the mechanism' do
      expect(selector[:mechanism]).to eq('MONGODB-X509')
    end

    context 'when a username is not provided' do

      let(:user) do
        Mongo::Auth::User.new(
          database: Mongo::Database::ADMIN
        )
      end

      it 'does not set the username' do
        expect(selector[:user]).to be_nil
      end

      it 'sets the mechanism' do
        expect(selector[:mechanism]).to eq('MONGODB-X509')
      end
    end

    context 'when the username is nil' do

      let(:user) do
        Mongo::Auth::User.new(
          database: Mongo::Database::ADMIN,
          user: nil
        )
      end

      it 'does not set the username' do
        expect(selector.has_key?(:user)).to be(false)
      end

      it 'sets the mechanism' do
        expect(selector[:mechanism]).to eq('MONGODB-X509')
      end
    end
  end
end
