module PrimarySocket
  def self.included(base)
    base.class_eval do

      let(:primary_server) do
        client.cluster.next_primary
      end

      let(:primary_connection) do
        connection = primary_server.pool.checkout
        connection.connect!
        primary_server.pool.checkin(connection)
        connection
      end

      let(:primary_socket) do
        primary_connection.send(:socket)
      end
    end
  end
end
