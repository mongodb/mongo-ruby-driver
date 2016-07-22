require 'spec_helper'

describe Mongo::Operation::Write::RemoveUser do

  describe '#execute' do

    before do
      root_authorized_client.database.users.create(
        'durran',
        password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    let(:operation) do
      described_class.new(user_name: 'durran', db_name: TEST_DB)
    end

    context 'when user removal was successful' do

      let!(:response) do
        operation.execute(root_authorized_primary)
      end

      it 'removes the user from the database' do
        expect(response).to be_successful
      end
    end

    context 'when removal was not successful' do

      before do
        operation.execute(root_authorized_primary)
      end

      it 'raises an exception', if: write_command_enabled? do
        expect {
          operation.execute(root_authorized_primary)
        }.to raise_error(Mongo::Error::OperationFailure)
      end

      it 'does not raise an exception', unless: write_command_enabled? do
        expect(operation.execute(root_authorized_primary).written_count).to eq(0)
      end
    end
  end
end
