# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Server::ConnectionPool::GenerationManager do
  describe '#close_all_pipes' do
    let(:service_id) { 'test_service_id' }

    let(:server) { instance_double('Mongo::Server') }

    let(:manager) { described_class.new(server: server) }

    before do
      manager.pipe_fds(service_id: service_id)
    end

    it 'closes all pipes' do
      expect(manager.pipe_fds(service_id: service_id).size).to eq(2)

      manager.instance_variable_get(:@pipe_fds)[service_id].each do |gen, (r, w)|
        expect(r).to receive(:close)
        expect(w).to receive(:close)
      end

      manager.close_all_pipes
    end

    it 'removes all pipes from the map' do
      expect(manager.pipe_fds(service_id: service_id).size).to eq(2)

      manager.instance_variable_get(:@pipe_fds)[service_id].each do |gen, (r, w)|
        expect(r).to receive(:close)
        expect(w).to receive(:close)
      end

      manager.close_all_pipes
    end
  end
end
