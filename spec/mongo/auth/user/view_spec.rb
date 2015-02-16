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

  describe '#update' do

  end
end
