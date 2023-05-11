# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'fork reconnect' do
  require_fork
  require_mri

  # On multi-shard sharded clusters a succeeding write request does not
  # guarantee that the next operation will succeed (since it could be sent to
  # another shard with a dead connection).
  require_no_multi_mongos

  require_stress

  let(:client) { authorized_client }

  describe 'client' do
    it 'works after fork' do
      # Perform a write so that we discover the current primary.
      # Previous test may have stepped down the server that authorized client
      # considers the primary.
      # In standalone deployments there are no retries, hence execute the
      # operation twice manually.
      client['foo'].insert_one(test: 1) rescue nil
      client['foo'].insert_one(test: 1)

      pids = []
      deadline = Mongo::Utils.monotonic_time + 5
      1.upto(10) do
        if pid = fork
          pids << pid
        else
          Utils.wrap_forked_child do
            while Mongo::Utils.monotonic_time < deadline
              client.database.command(hello: 1).should be_a(Mongo::Operation::Result)
            end
          end
        end
      end

      while Mongo::Utils.monotonic_time < deadline
        # Use a read which is retried in case of an error
        client['foo'].find(test: 1).to_a
      end

      pids.each do |pid|
        pid, status = Process.wait2(pid)
        status.exitstatus.should == 0
      end
    end

    retry_test
    context 'when parent is operating on client during the fork' do
      # This test intermittently fails in evergreen with pool size of 5,
      # with a number of pending connections in the pool.
      # The reason could be that handshaking is slow or operations are slow
      # post handshakes.
      # Sometimes it seems the monitoring connection experiences network
      # errors (despite being a loopback connection) which causes the test
      # to fail as then server selection fails.
      # The retry_test is to deal with network errors on monitoring connection.

      let(:client) { authorized_client.with(max_pool_size: 10,
        wait_queue_timeout: 10, socket_timeout: 2, connect_timeout: 2) }

      it 'works' do
        client.database.command(hello: 1).should be_a(Mongo::Operation::Result)

        threads = []
        5.times do
          threads << Thread.new do
            loop do
              client['foo'].find(test: 1).to_a
            end
          end
        end

        pids = []
        deadline = Mongo::Utils.monotonic_time + 5
        10.times do
          if pid = fork
            pids << pid
          else
            Utils.wrap_forked_child do
              while Mongo::Utils.monotonic_time < deadline
                client.database.command(hello: 1).should be_a(Mongo::Operation::Result)
              end
            end
          end
        end

        while Mongo::Utils.monotonic_time < deadline
          sleep 0.1
        end

        threads.map(&:kill)
        threads.map(&:join)

        pids.each do |pid|
          pid, status = Process.wait2(pid)
          status.exitstatus.should == 0
        end
      end
    end
  end
end
