# frozen_string_literal: true

require 'spec_helper'

describe 'Client backpressure options' do
  describe 'maxAdaptiveRetries' do
    describe 'client option' do
      it 'defaults to nil (uses policy default of 2)' do
        client = new_local_client_nmio([ 'localhost:27017' ])
        expect(client.options[:max_adaptive_retries]).to be_nil
      end

      it 'can be set to an integer' do
        client = new_local_client_nmio([ 'localhost:27017' ], max_adaptive_retries: 5)
        expect(client.options[:max_adaptive_retries]).to eq(5)
      end

      it 'can be set to 0' do
        client = new_local_client_nmio([ 'localhost:27017' ], max_adaptive_retries: 0)
        expect(client.options[:max_adaptive_retries]).to eq(0)
      end
    end

    describe 'URI option' do
      it 'parses maxAdaptiveRetries=3' do
        client = new_local_client_nmio('mongodb://localhost:27017/?maxAdaptiveRetries=3')
        expect(client.options[:max_adaptive_retries]).to eq(3)
      end

      it 'parses maxAdaptiveRetries=0' do
        client = new_local_client_nmio('mongodb://localhost:27017/?maxAdaptiveRetries=0')
        expect(client.options[:max_adaptive_retries]).to eq(0)
      end
    end

    it 'configures the retry policy max_retries' do
      client = new_local_client_nmio([ 'localhost:27017' ], max_adaptive_retries: 4)
      expect(client.retry_policy.max_retries).to eq(4)
    end

    context 'when derived via Client#with' do
      let(:client) { new_local_client_nmio([ 'localhost:27017' ], max_adaptive_retries: 4) }

      it 'rebuilds the retry policy with the new value' do
        expect(client.with(max_adaptive_retries: 1).retry_policy.max_retries).to eq(1)
      end

      it 'reverts to the default when the option is removed' do
        expect(client.with(max_adaptive_retries: nil).retry_policy.max_retries)
          .to eq(Mongo::Retryable::Backpressure::DEFAULT_MAX_RETRIES)
      end

      it 'leaves the original client policy alone' do
        client.with(max_adaptive_retries: 1)
        expect(client.retry_policy.max_retries).to eq(4)
      end

      it 'keeps the policy when an unrelated option changes' do
        derived = client.with(read: { mode: :secondary })
        expect(derived.retry_policy.max_retries).to eq(4)
      end
    end
  end

  describe 'enableOverloadRetargeting' do
    describe 'client option' do
      it 'defaults to nil (false)' do
        client = new_local_client_nmio([ 'localhost:27017' ])
        expect(client.options[:enable_overload_retargeting]).to be_nil
      end

      it 'can be set to true' do
        client = new_local_client_nmio([ 'localhost:27017' ], enable_overload_retargeting: true)
        expect(client.options[:enable_overload_retargeting]).to be true
      end
    end

    describe 'URI option' do
      it 'parses enableOverloadRetargeting=true' do
        client = new_local_client_nmio('mongodb://localhost:27017/?enableOverloadRetargeting=true')
        expect(client.options[:enable_overload_retargeting]).to be true
      end

      it 'parses enableOverloadRetargeting=false' do
        client = new_local_client_nmio('mongodb://localhost:27017/?enableOverloadRetargeting=false')
        expect(client.options[:enable_overload_retargeting]).to be false
      end
    end
  end
end
