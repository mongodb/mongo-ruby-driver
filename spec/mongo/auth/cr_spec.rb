require 'spec_helper'

describe Mongo::Auth::CR do

  let(:server) do
    authorized_client.cluster.next_primary
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.test_options)
  end

  describe '#login' do

    before do
      connection.connect!
    end

    context 'when the user is not authorized' do

      let(:user) do
        Mongo::Auth::User.new(
          database: 'driver',
          user: 'notauser',
          password: 'password'
        )
      end

      let(:cr) do
        described_class.new(user, connection)
      end

      let(:login) do
        cr.login.documents[0]
      end

      it 'raises an exception' do
        expect {
          cr.login
        }.to raise_error(Mongo::Auth::Unauthorized)
      end

      context 'when compression is used' do
        require_compression
        min_server_fcv '3.6'

        it 'does not compress the message' do
          expect(Mongo::Protocol::Compressed).not_to receive(:new)
          expect {
            cr.login
          }.to raise_error(Mongo::Auth::Unauthorized)
        end
      end
    end
  end

  context 'when the user is authorized for the database' do
    max_server_fcv '2.6'

    before do
      connection.connect!
    end

    let(:cr) do
      described_class.new(root_user, connection)
    end

    let(:login) do
      cr.login
    end

    it 'logs the user into the connection' do
      expect(login['ok']).to eq(1)
    end
  end
end
