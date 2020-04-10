require 'spec_helper'

describe Mongo::Operation::RemoveUser do

  describe '#execute' do

    before do
      users = root_authorized_client.database.users
      if users.info('durran').any?
        users.remove('durran')
      end
      users.create(
        'durran',
        password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    let(:operation) do
      described_class.new(user_name: 'durran', db_name: SpecConfig.instance.test_db)
    end

    let!(:response) do
      root_authorized_primary.with_connection do |connection|
        operation.execute(connection, client: nil)
      end
    end

    context 'when user removal was successful' do
      it 'removes the user from the database' do
        expect(response).to be_successful
      end
    end

    context 'when removal was not successful' do
      it 'raises an exception' do
        expect {
          root_authorized_primary.with_connection do |connection|
            operation.execute(connection, client: nil)
          end
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
