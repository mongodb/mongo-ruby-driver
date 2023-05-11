# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Gssapi::Conversation do
  require_mongo_kerberos

  let(:user) do
    Mongo::Auth::User.new(user: 'test')
  end

  let(:conversation) do
    described_class.new(user, 'test.example.com')
  end

  let(:authenticator) do
    double('authenticator')
  end

  let(:connection) do
    double('connection')
  end

  before do
    expect(Mongo::Auth::Gssapi::Authenticator).to receive(:new).
      with(user, 'test.example.com').
      and_return(authenticator)
  end

  context 'when the user has a realm', if: RUBY_PLATFORM == 'java' do

    let(:user) do
      Mongo::Auth::User.new(user: 'user1@MYREALM.ME')
    end

    it 'includes the realm in the username as it was provided' do
      expect(conversation.user.name).to eq(user.name)
    end
  end

  describe '#start' do

    let(:query) do
      conversation.start(connection)
    end

    let(:selector) do
      query.selector
    end

    before do
      expect(authenticator).to receive(:initialize_challenge).and_return('test')
    end

    it 'sets the sasl start flag' do
      expect(selector[:saslStart]).to eq(1)
    end

    it 'sets the auto authorize flag' do
      expect(selector[:autoAuthorize]).to eq(1)
    end

    it 'sets the mechanism' do
      expect(selector[:mechanism]).to eq('GSSAPI')
    end

    it 'sets the payload', unless: BSON::Environment.jruby? do
      expect(selector[:payload]).to start_with('test')
    end

    it 'sets the payload', if: BSON::Environment.jruby? do
      expect(selector[:payload].data).to start_with('test')
    end
  end

  describe '#finalize' do

    let(:continue_token) do
      BSON::Environment.jruby? ? BSON::Binary.new('testing') : 'testing'
    end

    context 'when the conversation is a success' do

      let(:reply_document) do
        BSON::Document.new(
          'conversationId' => 1,
          'done' => false,
          'payload' => continue_token,
          'ok' => 1.0,
        )
      end

      let(:query) do
        conversation.finalize(reply_document, connection)
      end

      let(:selector) do
        query.selector
      end

      before do
        expect(authenticator).to receive(:evaluate_challenge).
          with('testing').and_return(continue_token)
      end

      it 'sets the conversation id' do
        expect(selector[:conversationId]).to eq(1)
      end

      it 'sets the payload', unless: BSON::Environment.jruby? do
        expect(selector[:payload]).to eq(continue_token)
      end

      it 'sets the payload', if: BSON::Environment.jruby? do
        expect(selector[:payload].data).to eq(continue_token)
      end

      it 'sets the continue flag' do
        expect(selector[:saslContinue]).to eq(1)
      end
    end
  end
end
