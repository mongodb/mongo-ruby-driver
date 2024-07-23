# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Oidc::Conversation do
  let(:user) do
    Mongo::Auth::User.new(user: 'test')
  end

  let(:connection) do
    double('connection')
  end

  let(:conversation) do
    described_class.new(user, connection)
  end

  let(:features) do
    double('features')
  end

  describe '#start' do
    before do
      expect(connection).to receive(:features).and_return(features)
      expect(connection).to receive(:mongos?).and_return(false)
      expect(features).to receive(:op_msg_enabled?).and_return(true)
    end

    let(:token) do
      'token'
    end

    let(:msg) do
      conversation.start(connection: connection, token: token)
    end

    let(:selector) do
      msg.instance_variable_get(:@main_document)
    end

    it 'sets the sasl start flag' do
      expect(selector[:saslStart]).to eq(1)
    end

    it 'sets the mechanism' do
      expect(selector[:mechanism]).to eq('MONGODB-OIDC')
    end

    it 'sets the payload' do
      expect(selector[:payload].data).to eq({ jwt: token }.to_bson.to_s)
    end
  end
end
