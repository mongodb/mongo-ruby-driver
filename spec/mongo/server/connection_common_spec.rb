# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Server::ConnectionCommon do
  let(:subject) { described_class.new }

  let(:metadata) do
    Mongo::Server::AppMetadata.new({})
  end

  describe '#handshake_document' do
    let(:document) do
      subject.handshake_document(metadata)
    end

    context 'with api version' do
      let(:metadata) do
        Mongo::Server::AppMetadata.new({
          server_api: { version: '1'  }
        })
      end

      it 'returns hello document with API version' do
        expect(document['hello']).to eq(1)
      end
    end

    context 'without api version' do
      it 'returns legacy hello document without API version' do
        expect(document['isMaster']).to eq(1)
      end
    end

    context 'when connecting to load balancer' do

      let(:document) do
        subject.handshake_document(metadata, load_balancer: true)
      end

      it 'includes loadBalanced: true' do
        document['loadBalanced'].should be true
      end
    end
  end

  describe '#handshake_command' do
    let(:document) do
      subject.handshake_document(metadata, load_balancer: load_balancer)
    end
    
    let(:load_balancer) { false }

    context 'with api version' do
      let(:metadata) do
        Mongo::Server::AppMetadata.new({
          server_api: { version: '1'  }
        })
      end

      it 'returns OP_MSG command' do
        expect(
          subject.handshake_command(document)
        ).to be_a(Mongo::Protocol::Msg)
      end
    end
    
    context 'with loadBalanced=true' do
      let(:load_balancer) { true }

      it 'returns OP_MSG command' do
        expect(
          subject.handshake_command(document)
        ).to be_a(Mongo::Protocol::Msg)
      end
    end

    context 'without api version' do
      it 'returns OP_QUERY command' do
        expect(
          subject.handshake_command(document)
        ).to be_a(Mongo::Protocol::Query)
      end
    end
  end
end
