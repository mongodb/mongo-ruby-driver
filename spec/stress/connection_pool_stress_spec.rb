# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Connection pool stress test' do
  require_stress

  let(:options) do
    { max_pool_size: 5, min_pool_size: 3 }
  end

  let(:thread_count) { 5 }

  let(:documents) do
    [].tap do |documents|
      10000.times do |i|
        documents << { a: i}
      end
    end
  end

  let(:operation_threads) do
    [].tap do |threads|
      thread_count.times do |i|
        threads << Thread.new do
          100.times do |j|
            collection.find(a: i+j).to_a
            sleep 0.1
            collection.find(a: i+j).to_a
          end
        end
      end
    end
  end

  let(:client) do
    authorized_client.with(options)
  end

  let(:collection) do
    client[authorized_collection.name].tap do |collection|
      collection.drop
      collection.insert_many(documents)
    end
  end

  shared_examples_for 'does not raise error' do
    it 'does not raise error' do
      collection

      expect {
        threads.collect { |t| t.join }
      }.not_to raise_error
    end
  end

  describe 'when several threads run operations on the collection' do
    let(:threads) { operation_threads }

    context 'min pool size 0, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 0 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 1, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 1 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 2, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 2 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 3, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 3 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 4, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 4 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end

    context 'min pool size 5, max pool size 5' do
      let(:options) do
        { max_pool_size: 5, min_pool_size: 5 }
      end
      let(:thread_count) { 7 }

      it_behaves_like 'does not raise error'
    end
  end

  describe 'when there are many more threads than the max pool size' do
    let(:threads) { operation_threads }

    context '10 threads, max pool size 5' do
      let(:thread_count) { 10 }

      it_behaves_like 'does not raise error'
    end

    context '15 threads, max pool size 5' do
      let(:thread_count) { 15 }

      it_behaves_like 'does not raise error'
    end

    context '20 threads, max pool size 5' do
      let(:thread_count) { 20 }

      it_behaves_like 'does not raise error'
    end

    context '25 threads, max pool size 5' do
      let(:thread_count) { 25 }

      it_behaves_like 'does not raise error'
    end
  end
end
