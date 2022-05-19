# frozen_string_literal: true
# encoding: utf-8

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
  end
end
