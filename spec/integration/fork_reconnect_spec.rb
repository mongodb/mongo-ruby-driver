require 'spec_helper'

describe 'fork reconnect' do
  only_mri

  let(:client) { authorized_client }
  let(:server) { client.cluster.next_primary }

  describe 'monitoring connection' do
    let(:connection) do
      Mongo::Server::Monitor::Connection.new(server.address, server.options)
    end

    let(:operation) do
      connection.ismaster.should be_a(Hash)
    end

    it 'reconnects' do
      connection.connect!

      socket = connection.send(:socket).send(:socket)
      (socket.is_a?(Socket) || socket.is_a?(OpenSSL::SSL::SSLSocket)).should be true

      if pid = fork
        Process.wait(pid)
        $?.exitstatus.should == 0
      else
        operation

        child_socket = connection.send(:socket).send(:socket)
        # fileno of child_socket may equal to fileno of socket,
        # as socket would've been closed first and file descriptors can be
        # reused by the kernel.
        child_socket.object_id.should_not == socket.object_id

        # Exec so that we do not close any clients etc. in the child.
        exec('/bin/true')
      end

      # Connection should remain serviceable in the parent.
      # The operation here will be invoked again, since the earlier invocation
      # was in the child process.
      operation

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
        Process.wait(pid)
        $?.exitstatus.should == 0
      else
        operation

        child_socket = connection.send(:socket).send(:socket)
        # fileno of child_socket may equal to fileno of socket,
        # as socket would've been closed first and file descriptors can be
        # reused by the kernel.
        child_socket.object_id.should == socket.object_id

        # Exec so that we do not close any clients etc. in the child.
        exec('/bin/true')
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
        Process.wait(pid)
        $?.exitstatus.should == 0
      else
        new_conn_id = server.with_connection do |connection|
          connection.id
        end

        new_conn_id.should_not == conn_id

        # Exec so that we do not close any clients etc. in the child.
        exec('/bin/true')
      end

      parent_conn_id = server.with_connection do |connection|
        connection.id
      end

      parent_conn_id.should == conn_id
    end
  end

  describe 'client' do
    it 'works after fork' do
      client.cluster.next_primary.pool.clear
      client.database.command(ismaster: 1).should be_a(Mongo::Operation::Result)

      if pid = fork
        Process.wait(pid)
        $?.exitstatus.should == 0
      else
        client.database.command(ismaster: 1).should be_a(Mongo::Operation::Result)

        # Exec so that we do not close any clients etc. in the child.
        exec('/bin/true')
      end

      # Perform a read which can be retried, so that the socket close
      # performed by the child is recovered from.
      client['foo'].find(test: 1)
    end
  end
end
