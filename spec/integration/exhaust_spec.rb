require 'spec_helper'

describe 'Exhaust' do
  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  describe 'inserted_ids' do
    let(:client) do
      ClientRegistry.instance.global_client('authorized')
    end

    let(:collection) do
      client['exhaust'].with(exhaust_allowed: true)
    end

    before do
      arr = [{a: 1}]*1000
      #collection.insert_many(arr)
    end

    it 'works' do
      puts 'finding'
      #byebug
      view = collection.find({}, exhaust: true, batch_size: 10).to_enum

      # consume the first batch
      10.times do
        view.next
      end

      # at this point there should be only one connection in use
      server = client.cluster.next_primary
      queue = server.pool.send(:queue)
      expect(queue.pool_size).to eq(1)
      expect(queue.queue_size).to eq(1)

      # consume the next document, this should trigger an exhausting getMore
      view.next

      # still have one connection, but it is checked out
      expect(queue.pool_size).to eq(1)
      expect(queue.queue_size).to eq(0)

      # complete iteration
      expect do
        while true
          view.next
        end
      end.to raise_error(StopIteration)

      # connection should have been returned to the queue
      expect(queue.pool_size).to eq(1)
      expect(queue.queue_size).to eq(1)
    end
  end
end
