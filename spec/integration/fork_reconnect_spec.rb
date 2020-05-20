require 'spec_helper'

describe 'fork reconnect' do
  let(:client) { authorized_client }
  let(:server) { client.cluster.next_primary }

  shared_examples 'reconnects the connection' do
    it 'reconnects' do
      connection.connect!

      socket = connection.send(:socket).send(:socket)
      socket.should be_a(Socket)

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

      parent_socket = connection.send(:socket).send(:socket)
      # fileno of child_socket may equal to fileno of socket,
      # as socket would've been closed first and file descriptors can be
      # reused by the kernel.
      parent_socket.object_id.should == socket.object_id
    end
  end

  describe 'monitoring connection' do
    let(:connection) do
      Mongo::Server::Monitor::Connection.new(server.address)
    end

    let(:operation) do
      connection.ismaster.should be_a(Hash)
    end

    it_behaves_like 'reconnects the connection'
  end

  describe 'non-monitoring connection' do
    let(:connection) do
      Mongo::Server::Connection.new(server)
    end

    let(:operation) do
      connection.ping.should be true
    end

    it_behaves_like 'reconnects the connection'
  end
end
