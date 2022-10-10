require 'spec_helper'

describe Mongo::Auth::Azure::Credentials do
  describe '#initialize' do
    context 'when expires_in is not an Integer' do
      it 'raises an error' do
        expect do
          described_class.new(
            access_token: 'access_token',
            resource: 'https://management.azure.com/',
            token_type: 'Bearer',
            expires_in: 'error'
          )
        end.to raise_error(ArgumentError)
      end
    end
  end

  describe '#valid?' do
    context 'when expires in more than a minute in the future' do
      let(:subject) do
          described_class.new(
            access_token: 'access_token',
            resource: 'https://management.azure.com/',
            token_type: 'Bearer',
            expires_in: 60*10 # ten minutes
          )
      end

      it 'returns true' do
        expect(subject.valid?).to eq(true)
      end
    end

    context 'when expires in less then a minute in the future' do
      let(:subject) do
          described_class.new(
            access_token: 'access_token',
            resource: 'https://management.azure.com/',
            token_type: 'Bearer',
            expires_in: 30
          )
      end

      it 'returns false' do
        expect(subject.refresh_needed?).to eq(false)
      end
    end
  end
end
