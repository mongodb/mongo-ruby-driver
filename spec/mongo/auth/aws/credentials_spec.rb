# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::Auth::Aws::Credentials do
  describe '#expired?' do
    context 'when expiration is nil' do
      let(:credentials) do
        described_class.new('access_key_id', 'secret_access_key', nil, nil)
      end

      it 'returns false' do
        expect(credentials.expired?).to be false
      end
    end

    context 'when expiration is not nil' do
      before do
        Timecop.freeze
      end
      after do
        Timecop.return
      end
      context 'when the expiration is more than five minutes away' do
        let(:credentials) do
          described_class.new('access_key_id', 'secret_access_key', nil, Time.now.utc + 400)
        end

        it 'returns false' do
          expect(credentials.expired?).to be false
        end
      end

      context 'when the expiration is less than five minutes away' do
        let(:credentials) do
          described_class.new('access_key_id', 'secret_access_key', nil, Time.now.utc + 200)
        end

        it 'returns true' do
          expect(credentials.expired?).to be true
        end
      end
    end
  end
end
