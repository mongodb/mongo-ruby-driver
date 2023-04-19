# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe 'Mongo::Crypt::Binding' do
  describe 'helper methods' do
    require_libmongocrypt

    describe '#validate_document' do
      context 'with BSON::Document data' do
        it 'does not raise an exception' do
          expect do
            Mongo::Crypt::Binding.validate_document(BSON::Document.new)
          end.not_to raise_error
        end
      end

      context 'with Hash data' do
        it 'does not raise an exception' do
          expect do
            Mongo::Crypt::Binding.validate_document({})
          end.not_to raise_error
        end
      end

      context 'with nil data' do
        it 'raises an exception' do
          expect do
            Mongo::Crypt::Binding.validate_document(nil)
          end.to raise_error(Mongo::Error::CryptError, /Attempted to pass nil data to libmongocrypt/)
        end
      end

      context 'with non-document data' do
        it 'raises an exception' do
          expect do
            Mongo::Crypt::Binding.validate_document('not a bson document')
          end.to raise_error(Mongo::Error::CryptError, /Attempted to pass invalid data to libmongocrypt/)
        end
      end
    end
  end
end
