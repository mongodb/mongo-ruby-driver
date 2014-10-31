require 'spec_helper'

# b'user'
# pencil
# nonce
# b'NDA2NzU3MDY3MDYwMTgy'
# server_first
# b'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
# without_proof
# b'c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8'
# salted_pass
# b'B\x01\x8c\xdc-\xf7\xf2d\xb6,\xf3\xa5\x1b\xd1\xd8\xdb\xcb+f\x10'
# client_key
# b'\xe2s\xc1e/\x18\x1d\xae\x0f\xbc}T\x10\x12[k\x06\xe9\xbb0'
# stored_key
# b'/\x87\x8d\xd6\xe4.\x08\xaf\x8f\xe0m\xbf\xd7\x97\xc3\x0e\x82\x1f\xc93'
# auth_msg
# b'n=user,r=NDA2NzU3MDY3MDYwMTgy,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000,c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8'
# client_sig
# b'K\xf6\xd9R\x01\x8a^|a\xce\xd2?\x04\x1d<=\x039\xce\x83'
# client_proof
# b'p=qYUYNy6SQ9Jucq9rFA9nVgXQdbM='
# client_final
# b'c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,p=qYUYNy6SQ9Jucq9rFA9nVgXQdbM='

describe Mongo::Auth::SCRAM::Conversation do

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

  describe '#auth_message' do

    let(:auth_message) do
      conversation.send(:auth_message)
    end

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    it 'returns the auth message' do
      expect(auth_message).to eq(
        'n=user,r=NDA2NzU3MDY3MDYwMTgy,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,' +
        's=AVvQXzAbxweH2RYDICaplw==,i=10000,c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8'
      )
    end
  end

  describe '#client_empty_message' do

    let(:message) do
      conversation.send(:client_empty_message)
    end

    it 'returns an empty binary' do
      expect(message.data).to be_empty
    end
  end

  describe '#client_first_message' do

    let(:message) do
      conversation.send(:client_first_message)
    end

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
    end

    it 'returns the first client message' do
      expect(message.data).to eq('n,,n=user,r=NDA2NzU3MDY3MDYwMTgy')
    end
  end

  describe '#client_final_message' do

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    let(:message) do
      conversation.send(:client_final_message)
    end

    it 'returns the client final message' do
      expect(message.data).to eq(
        'c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,p=qYUYNy6SQ9Jucq9rFA9nVgXQdbM='
      )
    end
  end

  describe '#client_key' do

    let(:client_key) do
      conversation.send(:client_key, conversation.send(:salted_password)).force_encoding(BSON::UTF8)
    end

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    it 'returns the client key' do
      expect(client_key).to eq("\xE2s\xC1e/\x18\x1D\xAE\x0F\xBC}T\x10\x12[k\x06\xE9\xBB0")
    end
  end

  describe '#client_proof' do

    let(:key) do
      '1100001'
    end

    let(:signature) do
      '1100010'
    end

    let(:proof) do
      conversation.send(:client_proof, key, signature)
    end

    it 'encodes the xor combined strings' do
      expect(proof).to eq(Base64.strict_encode64("\x00\x00\x00\x00\x00\x01\x01"))
    end
  end

  describe '#first_bare' do

    let(:first_bare) do
      conversation.send(:first_bare)
    end

    before do
      expect(SecureRandom).to receive(:base64).once.and_return('NDA2NzU3MDY3MDYwMTgy')
    end

    it 'returns the first bare message' do
      expect(first_bare).to eq('n=user,r=NDA2NzU3MDY3MDYwMTgy')
    end
  end

  describe '#iterations' do

    let(:iterations) do
      conversation.send(:iterations)
    end

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    it 'returns the iterations' do
      expect(iterations).to eq(10000)
    end
  end

  describe '#rnonce' do

    let(:rnonce) do
      conversation.send(:rnonce)
    end

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    it 'returns the rnonce' do
      expect(rnonce).to eq('NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8')
    end
  end

  describe '#salt' do

    let(:salt) do
      conversation.send(:salt)
    end

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    it 'returns the salt' do
      expect(salt).to eq('AVvQXzAbxweH2RYDICaplw==')
    end
  end

  describe '#salted_password' do

    let(:salted_password) do
      conversation.send(:salted_password).force_encoding(BSON::UTF8)
    end

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    it 'returns the salted password' do
      expect(salted_password).to eq("B\x01\x8C\xDC-\xF7\xF2d\xB6,\xF3\xA5\e\xD1\xD8\xDB\xCB+f\x10")
    end
  end

  describe '#without_proof' do

    let(:without_proof) do
      conversation.send(:without_proof)
    end

    let(:payload) do
      BSON::Binary.new(
        'r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8,s=AVvQXzAbxweH2RYDICaplw==,i=10000'
      )
    end

    let(:reply) do
      Mongo::Protocol::Reply.new
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
      reply.instance_variable_set(:@documents, documents)
      conversation.instance_variable_set(:@nonce, 'NDA2NzU3MDY3MDYwMTgy')
      conversation.instance_variable_set(:@reply, reply)
    end

    it 'returns the without proof message' do
      expect(without_proof).to eq('c=biws,r=NDA2NzU3MDY3MDYwMTgyt7/+IWaw1HaZZ5NmPJUTWapLpH2Gg+d8')
    end
  end

  describe '#xor' do

    let(:first) do
      '1100001'
    end

    let(:second) do
      '1100010'
    end

    let(:xor) do
      conversation.send(:xor, first, second)
    end

    it 'encodes the xor combined strings' do
      expect(xor).to eq("\x00\x00\x00\x00\x00\x01\x01")
    end
  end

end
