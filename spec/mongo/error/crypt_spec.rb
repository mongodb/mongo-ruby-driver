require 'lite_spec_helper'

describe Mongo::Error::CryptError do
  let(:label) { :error_client }
  let(:code) { 401 }
  let(:message) { 'Operation unauthorized' }

  let(:error) { described_class.new(code, message) }

  describe '#initialize' do
    it 'properly populates fields' do
      expect(error.message).to eq("#{message} (libmongocrypt error code #{code})")
    end
  end
end
