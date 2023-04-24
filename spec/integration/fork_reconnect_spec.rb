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

  let(:client) { authorized_client }
  let(:server) { client.cluster.next_primary }

  describe 'monitoring connection' do
    let(:monitor) do
      Mongo::Server::Monitor.new(server, [], Mongo::Monitoring.new, server.options.merge(
        app_metadata: client.cluster.monitor_app_metadata,
        push_monitor_app_metadata: client.cluster.push_monitor_app_metadata,
      ))
    end

    it 'reconnects' do
      monitor.send(:do_scan).should be_a(Hash)

      socket = monitor.connection.send(:socket).send(:socket)
      (socket.is_a?(Socket) || socket.is_a?(OpenSSL::SSL::SSLSocket)).should be true

      if pid = fork
        pid, status = Process.wait2(pid)
        status.exitstatus.should == 0
      else
        monitor.send(:do_scan).should be_a(Hash)

        child_socket = monitor.connection.send(:socket).send(:socket)
        # fileno of child_socket may equal to fileno of socket,
        # as socket would've been closed first and file descriptors can be
        # reused by the kernel.
        child_socket.object_id.should_not == socket.object_id

        # Exec so that we do not close any clients etc. in the child.
        exec(Utils::BIN_TRUE)
      end

      # Connection should remain serviceable in the parent.
      # The operation here will be invoked again, since the earlier invocation
      # was in the child process.
      monitor.send(:do_scan).should be_a(Hash)

      # The child closes the connection's socket, but this races with the
      # parent. The parent can retain the original socket for a while.
    end
  end

  describe 'non-monitoring connection' do
    let(:connection) do
      Mongo::Server::Connection.new(server, server.options)
    end

    let(:operation) do
      connection.ping.should be true
    end

    it 'does not reconnect' do
      connection.connect!

      socket = connection.send(:socket).send(:socket)
      (socket.is_a?(Socket) || socket.is_a?(OpenSSL::SSL::SSLSocket)).should be true

      if pid = fork
        pid, status = Process.wait2(pid)
        status.exitstatus.should == 0
      else
        Utils.wrap_forked_child do
          operation

          child_socket = connection.send(:socket).send(:socket)
          # fileno of child_socket may equal to fileno of socket,
          # as socket would've been closed first and file descriptors can be
          # reused by the kernel.
          child_socket.object_id.should == socket.object_id
        end
      end

      # The child closes the connection's socket, but this races with the
      # parent. The parent can retain the original socket for a while.
    end
  end

  describe 'connection pool' do

    it 'creates a new connection in child' do
      conn_id = server.with_connection do |connection|
        connection.id
      end

      if pid = fork
        pid, status = Process.wait2(pid)
        status.exitstatus.should == 0
      else
        Utils.wrap_forked_child do
          new_conn_id = server.with_connection do |connection|
            connection.id
          end

          new_conn_id.should_not == conn_id
        end
      end

      parent_conn_id = server.with_connection do |connection|
        connection.id
      end

      parent_conn_id.should == conn_id
    end
  end

  describe 'client' do
    it 'works after fork' do
      # Perform a write so that we discover the current primary.
      # Previous test may have stepped down the server that authorized client
      # considers the primary.
      # In standalone deployments there are no retries, hence execute the
      # operation twice manually.
      client['foo'].insert_one(test: 1) rescue nil
      client['foo'].insert_one(test: 1)

      if pid = fork
        pid, status = Process.wait2(pid)
        status.exitstatus.should == 0
      else
        Utils.wrap_forked_child do
          client.database.command(ping: 1).should be_a(Mongo::Operation::Result)
        end
      end

      # Perform a read which can be retried, so that the socket close
      # performed by the child is recovered from.
      client['foo'].find(test: 1)
    end

    # Test from Driver Sessions Spec
    #   * Create ClientSession
    #   * Record its lsid
    #   * Delete it (so the lsid is pushed into the pool)
    #   * Fork
    #   * In the parent, create a ClientSession and assert its lsid is the same.
    #   * In the child, create a ClientSession and assert its lsid is different.
    describe 'session pool' do
      it 'is cleared after fork' do
        session = client.get_session.materialize_if_needed
        parent_lsid = session.session_id
        session.end_session

        if pid = fork
          pid, status = Process.wait2(pid)
          status.exitstatus.should == 0
        else
          Utils.wrap_forked_child do
            client.reconnect
            child_session = client.get_session.materialize_if_needed
            child_lsid = child_session.session_id
            expect(child_lsid).not_to eq(parent_lsid)
          end
        end

        session = client.get_session.materialize_if_needed
        session_id = session.session_id
        expect(session_id).to eq(parent_lsid)
      end

      # Test from Driver Sessions Spec
      #   * Create ClientSession
      #   * Record its lsid
      #   * Fork
      #   * In the parent, return the ClientSession to the pool, create a new
      #     ClientSession, and assert its lsid is the same.
      #   * In the child, return the ClientSession to the pool, create a new
      #     ClientSession, and assert its lsid is different.
      it 'does not return parent process sessions to child process pool' do
        session = client.get_session.materialize_if_needed
        parent_lsid = session.session_id

        if pid = fork
          pid, status = Process.wait2(pid)
          status.exitstatus.should == 0
        else
          Utils.wrap_forked_child do
            client.reconnect
            session.end_session
            child_session = client.get_session.materialize_if_needed

            child_lsid = child_session.session_id
            expect(child_lsid).not_to eq(parent_lsid)
          end
        end

        session.end_session
        session_id = client.get_session.materialize_if_needed.session_id
        expect(session_id).to eq(parent_lsid)
      end
    end
  end
end
