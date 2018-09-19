require 'spec_helper'

describe Mongo::Operation::CreateUser do

  describe '#execute' do

    let(:user) do
      Mongo::Auth::User.new(
        user: 'durran',
        password: 'password',
        roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    let(:operation) do
      described_class.new(user: user, db_name: SpecConfig.instance.test_db)
    end

    before do
      users = root_authorized_client.database.users
      if users.info('durran').any?
        users.remove('durran')
      end
    end

    context 'when user creation was successful' do

      let!(:response) do
        operation.execute(root_authorized_primary)
      end

      it 'saves the user in the database' do
        expect(response).to be_successful
      end
    end

    context 'when creation was not successful' do

      it 'raises an exception' do
        expect {
          operation.execute(root_authorized_primary)
          operation.execute(root_authorized_primary)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
