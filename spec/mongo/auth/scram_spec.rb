require 'spec_helper'

describe Mongo::Auth::SCRAM do

  let(:address) do
    default_address
  end

  let(:monitoring) do
    Mongo::Monitoring.new(monitoring: false)
  end

  let(:listeners) do
    Mongo::Event::Listeners.new
  end

  let(:cluster) do
    double('cluster').tap do |cl|
      allow(cl).to receive(:topology).and_return(topology)
      allow(cl).to receive(:app_metadata).and_return(app_metadata)
      allow(cl).to receive(:options).and_return({})
      allow(cl).to receive(:cluster_time).and_return(nil)
      allow(cl).to receive(:update_cluster_time)
    end
  end

  declare_topology_double

  let(:server) do
    Mongo::Server.new(address, cluster, monitoring, listeners,
      SpecConfig.instance.test_options
    ).tap do |server|
      server.scan!
    end
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.test_options)
  end

  context 'when SCRAM-SHA-1 is used' do
    min_server_fcv '3.0'

    describe '#login' do

      context 'when the user is not authorized' do

        let(:user) do
          Mongo::Auth::User.new(
            database: 'driver',
            user: 'notauser',
            password: 'password',
            auth_mech: :scram,
          )
        end

        let(:cr) do
          described_class.new(user)
        end

        it 'raises an exception' do
          expect {
            cr.login(connection)
          }.to raise_error(Mongo::Auth::Unauthorized)
        end

        context 'when compression is used' do
          require_compression
          min_server_fcv '3.6'

          it 'does not compress the message' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
            expect {
              cr.login(connection)
            }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end
      end

      context 'when the user is authorized for the database' do

        let(:cr) do
          described_class.new(root_user)
        end

        let(:login) do
          cr.login(connection).documents[0]
        end

        after do
          root_user.instance_variable_set(:@client_key, nil)
        end

        it 'logs the user into the connection and caches the client key' do
          expect(login['ok']).to eq(1)
          expect(root_user.send(:client_key)).not_to be_nil
        end

        it 'raises an exception when an incorrect client key is set' do
          root_user.instance_variable_set(:@client_key, "incorrect client key")
          expect {
            cr.login(connection)
          }.to raise_error(Mongo::Auth::Unauthorized)
        end
      end
    end
  end

  context 'when SCRAM-SHA-256 is used' do
    min_server_fcv '4.0'

    describe '#login' do

      context 'when the user is not authorized' do

        let(:user) do
          Mongo::Auth::User.new(
            database: 'driver',
            user: 'notauser',
            password: 'password',
            auth_mech: :scram256,
          )
        end

        let(:cr) do
          described_class.new(user)
        end

        it 'raises an exception' do
          expect {
            cr.login(connection)
          }.to raise_error(Mongo::Auth::Unauthorized)
        end

        context 'when compression is used' do
          require_compression
          min_server_fcv '3.6'

          it 'does not compress the message' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
            expect {
              cr.login(connection)
            }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end
      end

      context 'when the user is authorized for the database' do

        let(:cr) do
          described_class.new(test_user)
        end

        let(:login) do
          cr.login(connection).documents[0]
        end

        after do
          test_user.instance_variable_set(:@client_key, nil)
        end

        it 'logs the user into the connection and caches the client key' do
          expect(login['ok']).to eq(1)
          expect(test_user.send(:client_key)).not_to be_nil
        end

        it 'raises an exception when an incorrect client key is set' do
          test_user.instance_variable_set(:@client_key, "incorrect client key")
          expect {
            cr.login(connection)
          }.to raise_error(Mongo::Auth::Unauthorized)
        end
      end
    end
  end
end
