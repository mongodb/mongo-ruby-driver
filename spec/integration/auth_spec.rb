require 'spec_helper'

describe 'Auth' do
  describe 'Unauthorized exception message' do
    let(:server) do
      authorized_client.cluster.next_primary
    end

    let(:connection) do
      Mongo::Server::Connection.new(server, options)
    end

    before(:all) do
      # If auth is configured, the test suite uses the configured user
      # and does not create its own users. However, the configured user may
      # not have the auth mechanisms we need. Therefore we create a user
      # for this test without specifying auth mechanisms, which gets us
      # server default (scram for 4.0, scram & scram256 for 4.2).

      users = ClientRegistry.instance.global_client('root_authorized').use(:admin).database.users
      unless users.info('existing_user').empty?
        users.remove('existing_user')
      end
      users.create('existing_user', password: 'password')
    end

    context 'user mechanism not provided' do

      context 'user does not exist' do
        let(:options) { SpecConfig.instance.ssl_options.merge(
          user: 'nonexistent_user') }

        before do
          expect(connection.app_metadata.send(:document)[:saslSupportedMechs]).to eq('admin.nonexistent_user')
        end

        context 'scram-sha-1 only server' do
          min_server_fcv '3.0'
          max_server_version '3.6'

          it 'indicates scram-sha-1 was used' do
            expect do
              connection.connect!
            end.to raise_error(Mongo::Auth::Unauthorized, 'User nonexistent_user (mechanism: scram) is not authorized to access admin (used mechanism: SCRAM-SHA-1)')
          end
        end

        context 'scram-sha-256 server' do
          min_server_fcv '4.0'

          # An existing user on 4.0+ will negotiate scram-sha-256.
          # A non-existing user on 4.0+ will negotiate scram-sha-1.
          it 'indicates scram-sha-1 was used' do
            expect do
              connection.connect!
            end.to raise_error(Mongo::Auth::Unauthorized, 'User nonexistent_user (mechanism: scram) is not authorized to access admin (used mechanism: SCRAM-SHA-1)')
          end
        end
      end

      context 'user exists' do
        let(:options) { SpecConfig.instance.ssl_options.merge(
          user: 'existing_user', password: 'bogus') }

        before do
          expect(connection.app_metadata.send(:document)[:saslSupportedMechs]).to eq("admin.existing_user")
        end

        context 'scram-sha-1 only server' do
          min_server_fcv '3.0'
          max_server_version '3.6'

          it 'indicates scram-sha-1 was used' do
            expect do
              connection.connect!
            end.to raise_error(Mongo::Auth::Unauthorized, "User existing_user (mechanism: scram) is not authorized to access admin (used mechanism: SCRAM-SHA-1)")
          end
        end

        context 'scram-sha-256 server' do
          min_server_fcv '4.0'

          # An existing user on 4.0+ will negotiate scram-sha-256.
          # A non-existing user on 4.0+ will negotiate scram-sha-1.
          it 'indicates scram-sha-256 was used' do
            expect do
              connection.connect!
            end.to raise_error(Mongo::Auth::Unauthorized, "User existing_user (mechanism: scram256) is not authorized to access admin (used mechanism: SCRAM-SHA-256)")
          end
        end
      end
    end

    context 'user mechanism is provided' do
      min_server_fcv '3.0'

      context 'scram-sha-1 requested' do
        let(:options) { SpecConfig.instance.ssl_options.merge(
          user: 'nonexistent_user', auth_mech: :scram) }

        it 'indicates scram-sha-1 was requested and used' do
          expect do
            connection.connect!
          end.to raise_error(Mongo::Auth::Unauthorized, 'User nonexistent_user (mechanism: scram) is not authorized to access admin (used mechanism: SCRAM-SHA-1)')
        end
      end

      context 'scram-sha-256 requested' do
        min_server_fcv '4.0'

        let(:options) { SpecConfig.instance.ssl_options.merge(
          user: 'nonexistent_user', auth_mech: :scram256) }

        it 'indicates scram-sha-256 was requested and used' do
          expect do
            connection.connect!
          end.to raise_error(Mongo::Auth::Unauthorized, 'User nonexistent_user (mechanism: scram256) is not authorized to access admin (used mechanism: SCRAM-SHA-256)')
        end
      end
    end

    context 'attempting to connect to a non-tls server with tls' do
      require_no_tls

      let(:options) { {ssl: true} }

      it 'reports host, port and tls status' do
        begin
          connection.connect!
        rescue Mongo::Error::SocketError => exc
        end
        expect(exc).not_to be nil
        expect(exc.message).to include('OpenSSL::SSL::SSLError')
        expect(exc.message).to include(server.address.to_s)
        expect(exc.message).to include('TLS')
        expect(exc.message).not_to include('no TLS')
      end
    end

    context 'attempting to connect to a tls server without tls' do
      require_tls

      let(:options) { {} }

      it 'reports host, port and tls status' do
        begin
          connection.connect!
        rescue Mongo::Error::SocketError => exc
        end
        expect(exc).not_to be nil
        expect(exc.message).not_to include('OpenSSL::SSL::SSLError')
        addresses = Socket.getaddrinfo(server.address.host, nil)
        expect(addresses.any? do |address|
          exc.message.include?("#{address[2]}:#{server.address.port}")
        end).to be true
        expect(exc.message).to include('no TLS')
      end
    end
  end
end
