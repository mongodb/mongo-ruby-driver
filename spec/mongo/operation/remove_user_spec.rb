# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Operation::RemoveUser do
  require_no_required_api_version

  let(:context) { Mongo::Operation::Context.new }

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

    context 'when user removal was successful' do

      let!(:response) do
        operation.execute(root_authorized_primary, context: context)
      end

      it 'removes the user from the database' do
        expect(response).to be_successful
      end
    end

    context 'when removal was not successful' do

      before do
        operation.execute(root_authorized_primary, context: context)
      end

      it 'raises an exception' do
        expect {
          operation.execute(root_authorized_primary, context: context)
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end
end
