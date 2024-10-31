# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Error::PoolClearedError do
  describe '#initialize' do
    let(:error) do
      described_class.new(double('address'), double('pool'))
    end

    it 'appends TransientTransactionError' do
      expect(error.labels).to include('TransientTransactionError')
    end
  end
end
