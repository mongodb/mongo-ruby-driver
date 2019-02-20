require 'spec_helper'

# fails intermittently on jruby in Evergreen
describe Mongo::Auth::SCRAM::Conversation, retry: 3 do

  let(:conversation) do
    described_class.new(user, mechanism)
  end

  context 'when SCRAM-SHA-1 is used' do
    min_server_fcv '3.0'

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

      let(:query) do
        conversation.start
      end

      before do
        expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
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
        expect(selector[:mechanism]).to eq('SCRAM-SHA-1')
      end

      it 'sets the payload' do
        expect(selector[:payload].data).to eq('n,,n=user,r=NDA2NzU3MDY3MDYwMTgy')
      end
    end

    describe '#continue' do
      fails_on_jruby

      let(:reply) do
        Mongo::Protocol::Message.new
      end

      let(:documents) do
        [{
          'conversationId' => 1,
          'done' => false,
          'payload' => payload,
          'ok' => 1.0
        }]
      end

      before do
        expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
        allow(reply).to receive(:documents).and_return(documents)
      end

      context 'when the server rnonce starts with the nonce' do

        let(:payload) do
          BSON::Binary.new(
            'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
          )
        end

        let(:query) do
          conversation.continue(reply)
        end

        let(:selector) do
          query.selector
        end

        it 'sets the conversation id' do
          expect(selector[:conversationId]).to eq(1)
        end

        it 'sets the payload' do
          expect(selector[:payload].data).to eq(
            'c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,p=qYUYNy6SQ9Jucq9rFA9nVgXQdbM='
          )
        end

        it 'sets the continue flag' do
          expect(selector[:saslContinue]).to eq(1)
        end
      end

      context 'when the server nonce does not start with the nonce' do

        let(:payload) do
          BSON::Binary.new(
            'r=NDA2NzU4MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
          )
        end

        it 'raises an error' do
          expect {
            conversation.continue(reply)
          }.to raise_error(Mongo::Error::InvalidNonce)
        end
      end
    end

    describe '#finalize' do
      fails_on_jruby

      let(:continue_reply) do
        Mongo::Protocol::Message.new
      end

      let(:continue_documents) do
        [{
          'conversationId' => 1,
          'done' => false,
          'payload' => continue_payload,
          'ok' => 1.0
        }]
      end

      let(:continue_payload) do
        BSON::Binary.new(
          'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
        )
      end

      let(:reply) do
        Mongo::Protocol::Message.new
      end

      let(:documents) do
        [{
          'conversationId' => 1,
          'done' => false,
          'payload' => payload,
          'ok' => 1.0
        }]
      end

      before do
        expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
        allow(continue_reply).to receive(:documents).and_return(continue_documents)
        allow(reply).to receive(:documents).and_return(documents)
      end

      context 'when the verifier matches the server signature' do

        let(:payload) do
          BSON::Binary.new('v=gwo9E8+uifshm7ixj441GvIfuUY=')
        end

        let(:query) do
          conversation.continue(continue_reply)
          conversation.finalize(reply)
        end

        let(:selector) do
          query.selector
        end

        it 'sets the conversation id' do
          expect(selector[:conversationId]).to eq(1)
        end

        it 'sets the empty payload' do
          expect(selector[:payload].data).to eq('')
        end

        it 'sets the continue flag' do
          expect(selector[:saslContinue]).to eq(1)
        end
      end

      context 'when the verifier does not match the server signature' do

        let(:payload) do
          BSON::Binary.new('v=LQ+8yhQeVL2a3Dh+TDJ7xHz4Srk=')
        end

        it 'raises an error' do
          expect {
            conversation.continue(continue_reply)
            conversation.finalize(reply)
          }.to raise_error(Mongo::Error::InvalidSignature)
        end
      end
    end
  end

  context 'when SCRAM-SHA-256 is used' do
    min_server_fcv '4.0'

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

      let(:query) do
        conversation.start
      end

      before do
        expect(SecureRandom).to receive(:base64).once.and_return('rOprNGfwEbeRWgbNEkqO')
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
        expect(selector[:mechanism]).to eq('SCRAM-SHA-256')
      end

      it 'sets the payload' do
        expect(selector[:payload].data).to eq('n,,n=user,r=rOprNGfwEbeRWgbNEkqO')
      end
    end

    describe '#continue' do

      let(:reply) do
        Mongo::Protocol::Message.new
      end

      let(:documents) do
        [{
          'conversationId' => 1,
          'done' => false,
          'payload' => payload,
          'ok' => 1.0
        }]
      end

      before do
        expect(SecureRandom).to receive(:base64).once.and_return('rOprNGfwEbeRWgbNEkqO')
        allow(reply).to receive(:documents).and_return(documents)
      end

      context 'when the server rnonce starts with the nonce' do

        let(:payload) do
          BSON::Binary.new(
            'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
          )
        end

        let(:query) do
          conversation.continue(reply)
        end

        let(:selector) do
          query.selector
        end

        it 'sets the conversation id' do
          expect(selector[:conversationId]).to eq(1)
        end

        it 'sets the payload' do
          expect(selector[:payload].data).to eq(
            'c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ='
          )
        end

        it 'sets the continue flag' do
          expect(selector[:saslContinue]).to eq(1)
        end
      end

      context 'when the server nonce does not start with the nonce' do

        let(:payload) do
          BSON::Binary.new(
            'r=sOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
          )
        end

        it 'raises an error' do
          expect {
            conversation.continue(reply)
          }.to raise_error(Mongo::Error::InvalidNonce)
        end
      end
    end

    describe '#finalize' do

      let(:continue_reply) do
        Mongo::Protocol::Message.new
      end

      let(:continue_documents) do
        [{
          'conversationId' => 1,
          'done' => false,
          'payload' => continue_payload,
          'ok' => 1.0
        }]
      end

      let(:continue_payload) do
        BSON::Binary.new(
          'r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096'
        )
      end

      let(:reply) do
        Mongo::Protocol::Message.new
      end

      let(:documents) do
        [{
          'conversationId' => 1,
          'done' => false,
          'payload' => payload,
          'ok' => 1.0
        }]
      end

      before do
        expect(SecureRandom).to receive(:base64).once.and_return('rOprNGfwEbeRWgbNEkqO')
        allow(continue_reply).to receive(:documents).and_return(continue_documents)
        allow(reply).to receive(:documents).and_return(documents)
      end

      context 'when the verifier matches the server signature' do

        let(:payload) do
          BSON::Binary.new(' v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=')
        end

        let(:query) do
          conversation.continue(continue_reply)
          conversation.finalize(reply)
        end

        let(:selector) do
          query.selector
        end

        it 'sets the conversation id' do
          expect(selector[:conversationId]).to eq(1)
        end

        it 'sets the empty payload' do
          expect(selector[:payload].data).to eq('')
        end

        it 'sets the continue flag' do
          expect(selector[:saslContinue]).to eq(1)
        end
      end

      context 'when the verifier does not match the server signature' do

        let(:payload) do
          BSON::Binary.new('v=7rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=')
        end

        it 'raises an error' do
          expect {
            conversation.continue(continue_reply)
            conversation.finalize(reply)
          }.to raise_error(Mongo::Error::InvalidSignature)
        end
      end
    end
  end
end
