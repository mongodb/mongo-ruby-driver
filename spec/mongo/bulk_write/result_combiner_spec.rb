# frozen_string_literal: true

require 'spec_helper'

describe Mongo::BulkWrite::ResultCombiner do
  describe 'server_addresses accumulation' do
    let(:combiner) { described_class.new }

    def stub_op_result(seed)
      description = instance_double(
        Mongo::Server::Description,
        address: Mongo::Address.new(seed)
      )
      result = double('op_result')
      allow(result).to receive(:write_concern_error?).and_return(false)
      allow(result).to receive(:acknowledged?).and_return(true)
      allow(result).to receive(:aggregate_write_errors).and_return(nil)
      allow(result).to receive(:aggregate_write_concern_errors).and_return(nil)
      allow(result).to receive(:validate!).and_return(true)
      allow(result).to receive(:respond_to?).and_return(false)
      allow(result).to receive(:connection_description).and_return(description)
      result
    end

    it 'collects unique seeds from combined results' do
      combiner.combine!(stub_op_result('h1:27017'), 1)
      combiner.combine!(stub_op_result('h2:27017'), 1)
      combiner.combine!(stub_op_result('h1:27017'), 1)

      final = combiner.result
      expect(final.server_addresses).to contain_exactly('h1:27017', 'h2:27017')
    end

    it 'defaults to empty when no results combined' do
      expect(combiner.server_addresses).to eq([])
    end
  end
end
