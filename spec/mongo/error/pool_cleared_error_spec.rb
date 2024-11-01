# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Error::PoolClearedError do
  describe '#initialize' do
    let(:error) do
      described_class.new(
        instance_double(Mongo::Address), instance_double(Mongo::Server::ConnectionPool)
      )
    end

    it 'appends TransientTransactionError' do
      expect(error.labels).to include('TransientTransactionError')
    end
  end
end
