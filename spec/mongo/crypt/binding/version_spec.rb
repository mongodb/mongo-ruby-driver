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
end
