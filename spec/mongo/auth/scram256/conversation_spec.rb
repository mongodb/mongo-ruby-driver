# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require 'support/shared/scram_conversation'

describe Mongo::Auth::Scram256::Conversation do
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
      auth_mech: :scram256,
    )
  end

  let(:mechanism) do
    :scram256
  end

  describe '#start' do

    let(:msg) do
      conversation.start(connection)
    end

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('rOprNGfwEbeRWgbNEkqO')
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
      expect(command[:mechanism]).to eq('SCRAM-SHA-256')
    end

    it 'sets the payload' do
      expect(command[:payload].data).to eq('n,,n=user,r=rOprNGfwEbeRWgbNEkqO')
    end
  end

  describe '#continue' do
    include_context 'scram continue and finalize replies'

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('rOprNGfwEbeRWgbNEkqO')
    end

    context 'when the server rnonce starts with the nonce' do

      let(:continue_payload) do
        BSON::Binary.new(
          'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
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

      it 'sets the payload' do
        expect(command[:payload].data).to eq(
          'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ='
        )
      end

      it 'sets the continue flag' do
        expect(command[:saslContinue]).to eq(1)
      end
    end

    context 'when the server nonce does not start with the nonce' do

      let(:continue_payload) do
        BSON::Binary.new(
          'r=sOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
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
        'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
      )
    end

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('rOprNGfwEbeRWgbNEkqO')
    end

    context 'when the verifier matches the server signature' do

      let(:finalize_payload) do
        BSON::Binary.new(' v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=')
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

      it 'sets the empty payload' do
        expect(command[:payload].data).to eq('')
      end

      it 'sets the continue flag' do
        expect(command[:saslContinue]).to eq(1)
      end
    end

    context 'when the verifier does not match the server signature' do

      let(:finalize_payload) do
        BSON::Binary.new('v=7rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=')
      end

      it 'raises an error' do
        expect do
          conversation.continue(continue_document, connection)
          conversation.process_continue_response(finalize_document)
          conversation.finalize(connection)
        end.to raise_error(Mongo::Error::InvalidSignature)
      end
    end
  end
end
