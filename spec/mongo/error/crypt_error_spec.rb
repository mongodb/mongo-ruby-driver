# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Error::CryptError do
  let(:label) { :error_client }
  let(:code) { 401 }
  let(:message) { 'Operation unauthorized' }


  describe '#initialize' do
    context 'with code' do
      let(:error) { described_class.new(message, code: code) }

      it 'correctly generates the error message' do
        expect(error.message).to eq("#{message} (libmongocrypt error code #{code})")
      end
    end

    context 'with code' do
      let(:error) { described_class.new(message) }

      it 'correctly generates the error message' do
        expect(error.message).to eq(message)
      end
    end
  end
end
