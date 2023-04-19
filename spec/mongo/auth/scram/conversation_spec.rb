# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require 'support/shared/scram_conversation'

describe Mongo::Auth::Scram::Conversation do
  # Test uses global assertions
  clean_slate_for_all_if_possible

  include_context 'scram conversation context'

  let(:conversation) do
    described_class.new(user, double('connection'))
  end

  it_behaves_like 'scram conversation'

  let(:user) do
    Mongo::Auth::User.new(
      database: Mongo::Database::ADMIN,
      user: 'user',
      password: 'pencil',
      # We specify SCRAM-SHA-1 so that we don't accidentally use
      # SCRAM-SHA-256 on newer server versions.
      auth_mech: :scram,
    )
  end

  let(:mechanism) do
    :scram
  end

  describe '#start' do

    let(:msg) do
      conversation.start(connection)
    end

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
    end

    let(:command) do
      msg.payload['command']
    end

    it 'sets the sasl start flag' do
      expect(command[:saslStart]).to eq(1)
    end

    it 'sets the auto authorize flag' do
      expect(command[:autoAuthorize]).to eq(1)
    end

    it 'sets the mechanism' do
      expect(command[:mechanism]).to eq('SCRAM-SHA-1')
    end

    it 'sets the command' do
      expect(command[:payload].data).to eq('n,,n=user,r=NDA2NzU3MDY3MDYwMTgy')
    end
  end

  describe '#continue' do
    include_context 'scram continue and finalize replies'

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
    end

    context 'when the server rnonce starts with the nonce' do

      let(:continue_payload) do
        BSON::Binary.new(
          'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
        )
      end

      let(:msg) do
        conversation.continue(continue_document, connection)
      end

      let(:command) do
        msg.payload['command']
      end

      it 'sets the conversation id' do
        expect(command[:conversationId]).to eq(1)
      end

      it 'sets the command' do
        expect(command[:payload].data).to eq(
          'c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,p=qYUYNy6SQ9Jucq9rFA9nVgXQdbM='
        )
      end

      it 'sets the continue flag' do
        expect(command[:saslContinue]).to eq(1)
      end
    end

    context 'when the server nonce does not start with the nonce' do

      let(:continue_payload) do
        BSON::Binary.new(
          'r=NDA2NzU4MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
        )
      end

      it 'raises an error' do
        expect {
          conversation.continue(continue_document, connection)
        }.to raise_error(Mongo::Error::InvalidNonce)
      end
    end
  end

  describe '#finalize' do
    include_context 'scram continue and finalize replies'

    let(:continue_payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
    end

    context 'when the verifier matches the server signature' do

      let(:finalize_payload) do
        BSON::Binary.new('v=gwo9E8+uifshm7ixj441GvIfuUY=')
      end

      let(:msg) do
        conversation.continue(continue_document, connection)
        conversation.process_continue_response(finalize_document)
        conversation.finalize(connection)
      end

      let(:command) do
        msg.payload['command']
      end

      it 'sets the conversation id' do
        expect(command[:conversationId]).to eq(1)
      end

      it 'sets the empty command' do
        expect(command[:payload].data).to eq('')
      end

      it 'sets the continue flag' do
        expect(command[:saslContinue]).to eq(1)
      end
    end

    context 'when the verifier does not match the server signature' do

      let(:finalize_payload) do
        BSON::Binary.new('v=LQ+8yhQeVL2a3Dh+TDJ7xHz4Srk=')
      end

      it 'raises an error' do
        expect {
          conversation.continue(continue_document, connection)
          conversation.process_continue_response(finalize_document)
          conversation.finalize(connection)
        }.to raise_error(Mongo::Error::InvalidSignature)
      end
    end

    context 'when server signature is empty' do

      let(:finalize_payload) do
        BSON::Binary.new('v=')
      end

      it 'raises an error' do
        expect {
          conversation.continue(continue_document, connection)
          conversation.process_continue_response(finalize_document)
          conversation.finalize(connection)
        }.to raise_error(Mongo::Error::InvalidSignature)
      end
    end

    context 'when server signature is not provided' do

      let(:finalize_payload) do
        BSON::Binary.new('ok=absolutely')
      end

      it 'succeeds but does not mark conversation server verified' do
        conversation.continue(continue_document, connection)
        conversation.process_continue_response(finalize_document)
        conversation.finalize(connection)
        conversation.server_verified?.should be false
      end
    end
  end
end
