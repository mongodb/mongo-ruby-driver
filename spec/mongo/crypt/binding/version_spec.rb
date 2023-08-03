# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe 'Mongo::Crypt::Binding' do
  require_libmongocrypt

  describe '#mongocrypt_version' do
    let(:version) { Mongo::Crypt::Binding.mongocrypt_version(nil) }

    it 'is a string' do
      expect(version).to be_a_kind_of(String)
    end

    it 'is in the x.y.z-tag format' do
      expect(version).to match(/\A(\d+.){2}(\d+)?(-[A-Za-z\+\d]+)?\z/)
    end
  end

  describe '#validate_version' do
    context 'when not satisfied' do
      let(:older_version) do
        Mongo::Crypt::Binding::MIN_LIBMONGOCRYPT_VERSION.to_s.sub(/^\d+/, '0')
      end

      it 'raises an error' do
        expect do
          Mongo::Crypt::Binding.validate_version(older_version)
        end.to raise_error(LoadError, /libmongocrypt version .* or above is required, but version .* was found./)
      end
    end

    context 'when satisfied' do
      let(:newer_version) do
        Mongo::Crypt::Binding::MIN_LIBMONGOCRYPT_VERSION.bump.to_s
      end
      it 'does not raise and error' do
        expect do
          Mongo::Crypt::Binding.validate_version(newer_version)
        end.not_to raise_error(LoadError, /libmongocrypt version .* or above is required, but version .* was found./)
      end
    end

    context 'when in a non-parsable format' do
      let(:base_version) { Mongo::Crypt::Binding::MIN_LIBMONGOCRYPT_VERSION.to_s }

      shared_examples_for 'non-standard version format' do
        it 'does not raise an exception' do
          expect do
            Mongo::Crypt::Binding.validate_version(version)
          end.not_to raise_error
        end
      end

      context 'when the version is MAJOR.MINOR.PATH-dev+datecommit' do
        let(:version) { "#{base_version}-dev+20220730git8f8675fa11" }
        include_examples 'non-standard version format'
      end

      context 'when the version is MAJOR.MINOR.PATH-date+commit' do
        let(:version) { "#{base_version}-20230601+git9b07846bef" }
        include_examples 'non-standard version format'
      end
    end
  end
end
