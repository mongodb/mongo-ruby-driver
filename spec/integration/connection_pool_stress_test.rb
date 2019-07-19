require 'spec_helper'

describe 'Connection pool stress test' do
	let(:collection) do
		authorized_client.with(options)[authorized_collection.name]
	end

	let(:documents) do
		[].tap do |documents|
			100.times do |i|
				documents << { a: i}
			end
		end
	end

	let(:threads) do
		[].tap do |threads|
			thread_count.times do |i|
				threads << Thread.new do
					10.times do |j|
						collection.find(a: i+j)
						sleep 0.5
						collection.find(a: i+j)
					end
				end
			end
		end
	end

	before do
		collection.delete_many
		collection.insert_many(documents)
	end

	after do
		collection.delete_many
	end

	shared_examples_for 'does not raise error' do
		it 'does not raise error' do
			threads

			expect {
				threads.collect { |t| t.join }
			}.not_to raise_error
		end
	end

	describe 'min pool size less than max, fixed thread count' do
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
	end

	describe 'min pool size equal to max' do
		context 'thread count greater than max pool size' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 5 }
			end
			let(:thread_count) { 7 }

			it_behaves_like 'does not raise error'
		end

		context 'thread count equal to max pool size' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 5 }
			end
			let(:thread_count) { 5 }

			it_behaves_like 'does not raise error'
		end
	end

	describe 'thread count greater than max pool size' do
		context '6 threads, max pool size 5' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 3 }
			end
			let(:thread_count) { 6 }

			it_behaves_like 'does not raise error'
		end

		context '7 threads, max pool size 5' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 3 }
			end
			let(:thread_count) { 7 }

			it_behaves_like 'does not raise error'
		end

		context '8 threads, max pool size 5' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 3 }
			end
			let(:thread_count) { 8 }

			it_behaves_like 'does not raise error'
		end

		context '10 threads, max pool size 5' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 3 }
			end
			let(:thread_count) { 10 }

			it_behaves_like 'does not raise error'
		end

		context '15 threads, max pool size 5' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 3 }
			end
			let(:thread_count) { 15 }

			it_behaves_like 'does not raise error'
		end

		context '20 threads, max pool size 5' do
			let(:options) do
				{ max_pool_size: 5, min_pool_size: 3 }
			end
			let(:thread_count) { 20 }

			it_behaves_like 'does not raise error'
		end
	end

	describe 'when primary pool is disconnected' do
		let(:options) do
			{ max_pool_size: 5, min_pool_size: 3 }
		end

		let(:thread_count) { 5 }

		let(:threads) do
			threads = []

			# thread that performs operations
			threads << Thread.new do
				10.times do |j|
					collection.find(a: j)
					sleep 0.5
					collection.find(a: j)
				end
			end

			# thread that marks primary unknown and disconnects its pool
			threads << Thread.new do
				sleep 0.2
				server = authorized_client.cluster.next_primary
				server.unknown!
				server.pool.disconnect!
			end
		end

		context 'primary disconnected' do
			it_behaves_like 'does not raise error'
		end
	end

	describe 'when all pools are disconnected' do
		let(:options) do
			{ max_pool_size: 5, min_pool_size: 3 }
		end

		let(:thread_count) { 5 }

		let(:threads) do
			threads = []

			# thread that performs operations
			threads << Thread.new do
				10.times do |j|
					collection.find(a: j)
					sleep 0.5
					collection.find(a: j)
				end
			end

			# thread that marks primary unknown and disconnects its pool
			threads << Thread.new do
				sleep 0.2

				authorized_client.cluster.servers_list.each do |server|
					server.unknown!
					server.pool.disconnect!
				end
			end
		end

		context 'primary disconnected' do
			it_behaves_like 'does not raise error'
		end
	end
end
