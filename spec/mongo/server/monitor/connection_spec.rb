require 'spec_helper'

describe Mongo::Server::Monitor::Connection do

  let(:client) do
    authorized_client.with(options)
  end

  let(:address) do
    client.cluster.next_primary.address
  end

  declare_topology_double

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(Mongo::Server::Monitor::AppMetadata.new(authorized_client.cluster.options))
      allow(cl).to receive(:options).and_return({})
    end
  end

  let(:server) do
    Mongo::Server.new(address,
                      cluster,
                      Mongo::Monitoring.new(monitoring: false),
                      Mongo::Event::Listeners.new, options)
  end

  let(:connection) do
    server.monitor.connection
  end

  after do
    client.close
  end

  context 'when a connect_timeout is in the options' do

    context 'when a socket_timeout is in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: 3, socket_timeout: 5)
      end

      before do
        connection.connect!
      end

      it 'uses the connect_timeout for the address' do
        expect(connection.address.send(:connect_timeout)).to eq(3)
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(3)
      end
    end

    context 'when a socket_timeout is not in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: 3, socket_timeout: nil)
      end

      before do
        connection.connect!
      end

      it 'uses the connect_timeout for the address' do
        expect(connection.address.send(:connect_timeout)).to eq(3)
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(3)
      end
    end
  end

  context 'when a connect_timeout is not in the options' do

    context 'when a socket_timeout is in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: nil, socket_timeout: 5)
      end

      before do
        connection.connect!
      end

      it 'uses the default connect_timeout for the address' do
        expect(connection.address.send(:connect_timeout)).to eq(10)
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(10)
      end
    end

    context 'when a socket_timeout is not in the options' do

      let(:options) do
        SpecConfig.instance.test_options.merge(connect_timeout: nil, socket_timeout: nil)
      end

      before do
        connection.connect!
      end

      it 'uses the default connect_timeout for the address' do
        expect(connection.address.send(:connect_timeout)).to eq(10)
      end

      it 'uses the connect_timeout as the socket_timeout' do
        expect(connection.send(:socket).timeout).to eq(10)
      end
    end
  end
end
