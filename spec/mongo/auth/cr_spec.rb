require 'spec_helper'

describe Mongo::Auth::CR do

  let(:server) do
    authorized_client.cluster.next_primary
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.test_options)
  end

  describe '#login' do

    context 'when the user is not authorized' do

      let(:user) do
        Mongo::Auth::User.new(
          database: 'driver',
          user: 'notauser',
          password: 'password'
        )
      end

      let(:cr) do
        described_class.new(user)
      end

      let(:login) do
        cr.login(connection).documents[0]
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
  end

  context 'when the user is authorized for the database' do
    max_server_fcv '2.6'

    let(:cr) do
      described_class.new(root_user)
    end

    let(:login) do
      cr.login(connection).documents[0]
    end

    it 'logs the user into the connection' do
      expect(cr.login(connection).documents[0]['ok']).to eq(1)
    end
  end
end
