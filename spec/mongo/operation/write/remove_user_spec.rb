require 'spec_helper'

describe Mongo::Operation::Write::RemoveUser do

  describe '#execute' do

    before do
      authorized_client.database.users.create(
        'durran',
        'password',
        roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    let(:operation) do
      described_class.new(name: 'durran', db_name: TEST_DB)
    end

    let!(:response) do
      operation.execute(authorized_primary.context)
    end

    context 'when user removal was successful' do

      it 'saves the user in the database' do
        expect(response).to be_ok
      end
    end

    context 'when removal was not successful' do

      it 'raises an exception' do
        expect {
          operation.execute(authorized_primary.context)
        }.to raise_error(Mongo::Operation::Write::Failure)
      end
    end
  end
end
