require 'spec_helper'

describe 'Auth' do
  describe 'Unauthorized exception message' do
    let(:server) do
      authorized_client.cluster.next_primary
    end

    let(:connection) do
      Mongo::Server::Connection.new(server, options)
    end

    context 'user mechanism not provided' do

      let(:options) { {user: 'foo'} }

      context 'scram-sha-1 server' do
        min_server_fcv '3.0'

        context 'scram-sha-1 only server' do
          max_server_version '3.6'

          it 'indicates scram-sha-1 was used' do
            expect do
              connection.connect!
            end.to raise_error(Mongo::Auth::Unauthorized, 'User foo (mechanism: scram) is not authorized to access admin (used mechanism: SCRAM-SHA-1)')
          end
        end

        context 'scram-sha-1 requested' do
          let(:options) { {user: 'foo', auth_mech: :scram256} }

          it 'indicates scram-sha-1 was requested and used' do
            expect do
              connection.connect!
            end.to raise_error(Mongo::Auth::Unauthorized, 'User foo (mechanism: scram) is not authorized to access admin (used mechanism: SCRAM-SHA-1)')
          end
        end
      end

      context 'scram-sha-256 server' do
        min_server_fcv '4.0'

        it 'indicates scram-sha-256 was used' do
          expect do
            connection.connect!
          end.to raise_error(Mongo::Auth::Unauthorized, 'User foo (mechanism: scram) is not authorized to access admin (used mechanism: SCRAM-SHA-1)')
        end

        context 'scram-sha-256 requested' do
          let(:options) { {user: 'foo', auth_mech: :scram256} }

          it 'indicates scram-sha-256 was requested and used' do
            expect do
              connection.connect!
            end.to raise_error(Mongo::Auth::Unauthorized, 'User foo (mechanism: scram256) is not authorized to access admin (used mechanism: SCRAM-SHA-256)')
          end
        end
      end
    end
  end
end
