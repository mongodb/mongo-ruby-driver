require 'spec_helper'

describe Mongo::Server::Type do

  describe '.determine' do

    let(:server_type) do
      described_class.determine(description)
    end

    context 'when the description indicates an arbiter' do

      let(:ismaster) do
        { 'arbiterOnly' => true, 'setName' => 'test' }
      end

      let(:description) do
        Mongo::Server::Description.new(ismaster)
      end

      it 'returns arbiter' do
        expect(server_type).to eq(Mongo::Server::Type::ARBITER)
      end
    end

    context 'when the description indicates a ghost' do

      let(:ismaster) do
        { 'isreplicaset' => true }
      end

      let(:description) do
        Mongo::Server::Description.new(ismaster)
      end

      it 'returns ghost' do
        expect(server_type).to eq(Mongo::Server::Type::GHOST)
      end
    end

    context 'when the description indicates a mongos' do

      let(:ismaster) do
        { 'msg' => 'isdbgrid', 'ismaster' => true }
      end

      let(:description) do
        Mongo::Server::Description.new(ismaster)
      end

      it 'returns mongos' do
        expect(server_type).to eq(Mongo::Server::Type::MONGOS)
      end
    end

    context 'when the description indicates a primary' do

      let(:ismaster) do
        { 'setName' => 'test', 'ismaster' => true }
      end

      let(:description) do
        Mongo::Server::Description.new(ismaster)
      end

      it 'returns primary' do
        expect(server_type).to eq(Mongo::Server::Type::PRIMARY)
      end
    end

    context 'when the description indicates a secondary' do

      let(:ismaster) do
        { 'setName' => 'test', 'secondary' => true }
      end

      let(:description) do
        Mongo::Server::Description.new(ismaster)
      end

      it 'returns secondary' do
        expect(server_type).to eq(Mongo::Server::Type::SECONDARY)
      end
    end

    context 'when the description indicates an unknown' do

      let(:description) do
        Mongo::Server::Description.new({})
      end

      it 'returns unknown' do
        expect(server_type).to eq(Mongo::Server::Type::UNKNOWN)
      end
    end
  end
end
