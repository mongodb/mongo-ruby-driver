# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Oidc::TokenCache do
  let(:cache) do
    described_class.new
  end

  describe '#invalidate' do
    let(:token_one) do
      'token_one'
    end

    let(:token_two) do
      'token_two'
    end

    context 'when the token matches the existing token' do
      before do
        cache.access_token = token_one
        cache.invalidate(token: token_one)
      end

      it 'invalidates the token' do
        expect(cache.access_token).to be_nil
      end
    end

    context 'when the token does not equal the existing token' do
      before do
        cache.access_token = token_one
        cache.invalidate(token: token_two)
      end

      it 'does not invalidate the token' do
        expect(cache.access_token).to eq(token_one)
      end
    end
  end
end
