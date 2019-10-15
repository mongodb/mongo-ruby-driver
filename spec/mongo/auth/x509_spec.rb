require 'spec_helper'

describe Mongo::Auth::X509 do

  let(:server) do
    authorized_client.cluster.next_primary
  end

  let(:connection) do
    Mongo::Server::Connection.new(server, SpecConfig.instance.test_options)
  end

  let(:user) do
    Mongo::Auth::User.new(database: '$external')
  end

  describe '#initialize' do

    context 'when user specifies database $external' do

      let(:user) do
        Mongo::Auth::User.new(database: '$external')
      end

      it 'works' do
        described_class.new(user)
      end
    end

    context 'when user specifies database other than $external' do

      let(:user) do
        Mongo::Auth::User.new(database: 'foo')
      end

      it 'raises InvalidConfiguration' do
        expect do
          described_class.new(user)
        end.to raise_error(Mongo::Auth::InvalidConfiguration, /User specifies auth source 'foo', but the only valid auth source for X.509 is '\$external'/)
      end
    end
  end

  describe '#login' do

    context 'when the user is not authorized for the database' do

      let(:x509) do
        described_class.new(user)
      end

      let(:login) do
        x509.login(connection).documents[0]
      end

      it 'logs the user into the connection' do
        expect {
          x509.login(connection)
        }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end
  end
end
