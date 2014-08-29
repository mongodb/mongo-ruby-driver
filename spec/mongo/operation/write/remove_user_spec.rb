require 'spec_helper'

describe Mongo::Operation::Write::RemoveUser do

  describe '#execute' do

    before do
      authorized_client.database.users.create(
        'durran',
        password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    let(:operation) do
      described_class.new(name: 'durran', db_name: TEST_DB)
    end

    context 'when user removal was successful' do

      let!(:response) do
        operation.execute(authorized_primary.context)
      end

      it 'removes the user from the database' do
        expect(response).to be_ok
      end
    end

    context 'when removal was not successful' do

      before do
        operation.execute(authorized_primary.context)
      end

      it 'raises an exception', if: write_command_enabled? do
        expect {
          operation.execute(authorized_primary.context)
        }.to raise_error(Mongo::Operation::Write::Failure)
      end

      it 'does not raise an exception', unless: write_command_enabled? do
        expect(operation.execute(authorized_primary.context).n).to eq(0)
      end
    end
  end
end
