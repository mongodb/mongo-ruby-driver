# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Error::UnsupportedOption do
  describe '.allow_disk_use_error' do
    let(:error) { described_class.allow_disk_use_error }

    it 'creates an error with a default message' do
      expect(error.message).to eq(
        'The MongoDB server handling this request does not support the allow_disk_use ' \
        'option on this command. The allow_disk_use option is supported on find commands ' \
        'on MongoDB server versions 4.4 and later'
      )
    end
  end

  describe '.commit_quorum_error' do
    let(:error) { described_class.commit_quorum_error }

    it 'creates an error with a default message' do
      expect(error.message).to eq(
        'The MongoDB server handling this request does not support the commit_quorum ' \
        'option on this command. The commit_quorum option is supported on createIndexes commands ' \
        'on MongoDB server versions 4.4 and later'
      )
    end
  end
end
