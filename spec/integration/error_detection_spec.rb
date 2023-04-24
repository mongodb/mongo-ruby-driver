# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Error detection' do
  context 'document contains a not master/node recovering code' do
    let(:document) { {code: 91} }

    let(:coll) { authorized_client_without_any_retries['error-detection'] }

    before do
      coll.delete_many
    end

    context 'cursors not used' do

      before do
        coll.insert_one(document)
      end

      it 'is not treated as an error when retrieved' do
        actual = coll.find.first
        expect(actual['code']).to eq(91)
      end
    end

    context 'cursors used' do

      before do
        10.times do
          coll.insert_one(document)
        end
      end

      it 'is not treated as an error when retrieved' do
        actual = coll.find({}, batch_size: 2).first
        expect(actual['code']).to eq(91)
      end
    end
  end
end
