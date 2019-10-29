require 'lite_spec_helper'

describe Mongo::Error::CryptError do
  let(:label) { :error_client }
  let(:code) { 401 }
  let(:message) { 'Operation unauthorized' }

  let(:error) { described_class.new(code, message) }

  describe '#initialize' do
    it 'properly populates fields' do
      expect(error.code).to eq(code)
      expect(error.message).to eq(message)
    end
  end

  # describe '#self.from_status' do
  #   let(:status) do
  #     status = Mongo::Crypt::Status.new
  #     status.update(label, code, message)
  #   end

  #   let(:error) { described_class.from_status(status) }

  #   after do
  #     status.close
  #   end

  #   context 'with error status' do
  #     it 'returns an error based on status information' do
  #       expect(error.label).to eq(label)
  #       expect(error.code).to eq(code)
  #       expect(error.message).to eq(message)
  #     end
  #   end

  #   context 'with ok status' do
  #     let(:label) { :ok }
  #     let(:code) { 200 }
  #     let(:message) { 'Success' }

  #     it 'does not return an error' do
  #       expect(error).to be_nil
  #     end
  #   end
  # end
end
