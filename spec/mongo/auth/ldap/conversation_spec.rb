require 'spec_helper'

describe Mongo::Auth::LDAP::Conversation do

  let(:user) do
    Mongo::Auth::User.new(
      database: Mongo::Database::ADMIN,
      user: 'user',
      password: 'pencil'
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

    it 'sets the sasl start flag' do
      expect(selector[:saslStart]).to eq(1)
    end

    it 'sets the auto authorize flag' do
      expect(selector[:autoAuthorize]).to eq(1)
    end

    it 'sets the mechanism' do
      expect(selector[:mechanism]).to eq('PLAIN')
    end

    it 'sets the payload' do
      expect(selector[:payload].data).to eq("\x00user\x00pencil")
    end
  end
end
