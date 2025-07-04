# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Server::ConnectionPool::GenerationManager do
  describe '#close_all_pipes' do
    let(:service_id) { 'test_service_id' }

    let(:server) { instance_double(Mongo::Server) }

    let(:manager) { described_class.new(server: server) }

    before do
      manager.pipe_fds(service_id: service_id)
    end

    it 'closes all pipes and removes them from the map' do
      expect(manager.pipe_fds(service_id: service_id).size).to eq(2)

      manager.instance_variable_get(:@pipe_fds)[service_id].each do |_gen, (r, w)|
        expect(r).to receive(:close).and_call_original
        expect(w).to receive(:close).and_call_original
      end

      manager.close_all_pipes

      expect(manager.instance_variable_get(:@pipe_fds)).to be_empty
    end
  end
end
