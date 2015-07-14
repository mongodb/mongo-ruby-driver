require 'spec_helper'

describe Mongo::Auth::User::View do

  let(:view) do
    described_class.new(root_authorized_client.database)
  end

  describe '#create' do

    let!(:response) do
      view.create(
        'durran',
        password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    after do
      view.remove('durran')
    end

    context 'when user creation was successful' do

      it 'saves the user in the database' do
        expect(response).to be_successful
      end
    end

    context 'when creation was not successful' do

      it 'raises an exception' do
        expect {
          view.create('durran', password: 'password')
        }.to raise_error(Mongo::Error::OperationFailure)
      end
    end
  end

  describe '#update' do

    before do
      view.create(
          'durran',
          password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    after do
      view.remove('durran')
    end

    context 'when a user password is updated' do

      let!(:response) do
        view.update(
            'durran',
            password: '123', roles: [ Mongo::Auth::Roles::READ_WRITE ]
        )
      end

      it 'updates the password' do
        expect(response).to be_successful
      end
    end

    context 'when the roles of a user are updated' do

      let!(:response) do
        view.update(
            'durran',
            password: 'password', roles: [ Mongo::Auth::Roles::READ ]
        )
      end

      it 'updates the roles' do
        expect(response).to be_successful
      end
    end
  end

  describe '#remove' do

    context 'when user removal was successful' do

      before do
        view.create(
          'durran',
          password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
        )
      end

      let(:response) do
        view.remove('durran')
      end

      it 'saves the user in the database' do
        expect(response).to be_successful
      end
    end

    context 'when removal was not successful' do

      it 'raises an exception', if: write_command_enabled? do
        expect {
          view.remove('notauser')
        }.to raise_error(Mongo::Error::OperationFailure)
      end

      it 'does not raise an exception', unless: write_command_enabled? do
        expect(view.remove('notauser').written_count).to eq(0)
      end
    end
  end

  describe '#info' do

    context 'when a user exists in the database' do

      before do
        view.create(
            'emily',
            password: 'password'
        )
      end

      after do
        view.remove('emily')
      end

      it 'returns information for that user' do
        expect(view.info('emily')).to_not be_empty
      end
    end

    context 'when a user does not exist in the database' do

      it 'returns nil' do
        expect(view.info('emily')).to be_empty
      end
    end

  end
end
