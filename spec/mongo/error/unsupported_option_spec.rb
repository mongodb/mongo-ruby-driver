# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Error::UnsupportedOption do
  describe '.hint_error' do
    context 'with no options' do
      let(:error) { described_class.hint_error }

      it 'creates an error with a default message' do
        expect(error.message).to eq(
          "The MongoDB server handling this request does not support the hint " \
          "option on this command. The hint option is supported on update commands " \
          "on MongoDB server versions 4.2 and later and on findAndModify and delete " \
          "commands on MongoDB server versions 4.4 and later"
        )
      end

      context 'with unacknowledged_write: true' do
        let(:error) { described_class.hint_error(unacknowledged_write: true) }

        it 'creates an error with a default unacknowledged writes message' do
          expect(error.message).to eq(
            "The hint option cannot be specified on an unacknowledged " \
            "write operation. Remove the hint option or perform this " \
            "operation with a write concern of at least { w: 1 }"
          )
        end
      end
    end
  end

  describe '.allow_disk_use_error' do
    let(:error) { described_class.allow_disk_use_error }

    it 'creates an error with a default message' do
      expect(error.message).to eq(
        "The MongoDB server handling this request does not support the allow_disk_use " \
        "option on this command. The allow_disk_use option is supported on find commands " \
        "on MongoDB server versions 4.4 and later"
      )
    end
  end

  describe '.commit_quorum_error' do
    let(:error) { described_class.commit_quorum_error }

    it 'creates an error with a default message' do
      expect(error.message).to eq(
        "The MongoDB server handling this request does not support the commit_quorum " \
        "option on this command. The commit_quorum option is supported on createIndexes commands " \
        "on MongoDB server versions 4.4 and later"
      )
    end
  end
end
