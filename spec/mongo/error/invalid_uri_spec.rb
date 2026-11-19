# frozen_string_literal: true

require 'lite_spec_helper'

describe Mongo::Error::InvalidURI do
  describe '#initialize' do
    let(:details) { 'Invalid port' }

    context 'when the uri has cleartext credentials' do
      let(:uri) { 'mongodb://alice:s3cret@host:bad-port/admin' }

      let(:error) { described_class.new(uri, details) }

      it 'does not include the password in the message' do
        expect(error.message).not_to include('s3cret')
      end

      it 'does not include the username in the message' do
        expect(error.message).not_to include('alice')
      end

      it 'does not include any part of the uri in the message' do
        expect(error.message).not_to include('host:bad-port')
        expect(error.message).not_to include('bad-port')
      end

      it 'still includes the supplied details' do
        expect(error.message).to include(details)
      end
    end

    context 'when the uri is a mongodb+srv URI with credentials' do
      let(:uri) { 'mongodb+srv://alice:s3cret@cluster.example.com' }

      let(:error) { described_class.new(uri, details) }

      it 'does not include the password' do
        expect(error.message).not_to include('s3cret')
      end

      it 'does not include the host' do
        expect(error.message).not_to include('cluster.example.com')
      end
    end

    context 'when the uri has no credentials' do
      let(:uri) { 'mongodb://host:27017' }

      it 'does not include the uri' do
        error = described_class.new(uri, details)
        expect(error.message).not_to include(uri)
      end
    end

    context 'when the uri contains a password that was not delimited' do
      let(:uri) { 'mongodb://alice:s3cret' }

      it 'does not include the password' do
        expect(described_class.new(uri, details).message).not_to include('s3cret')
      end
    end

    context 'when the uri is nil' do
      it 'does not raise when constructing the message' do
        expect { described_class.new(nil, details) }.not_to raise_error
      end
    end
  end
end
