require 'spec_helper'

describe Mongo::Operation::Write::UpdateUser do

  describe '#execute' do

    let(:user) do
      Mongo::Auth::User.new(
          user: 'durran',
          password: 'password',
          roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    let(:user_updated) do
      Mongo::Auth::User.new(
          user: 'durran',
          password: '123',
          roles: [ Mongo::Auth::Roles::READ ]
      )
    end

    let(:operation) do
      described_class.new(user: user_updated, db_name: TEST_DB)
    end

    before do
      root_authorized_client.database.users.create(user)
    end

    after do
      root_authorized_client.database.users.remove('durran')
    end

    context 'when user update was successful' do

      let!(:response) do
        operation.execute(root_authorized_primary.context)
      end

      it 'updates the user in the database' do
        expect(response).to be_successful
      end
    end
  end
end
