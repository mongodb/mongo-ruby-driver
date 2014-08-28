require 'spec_helper'

describe Mongo::View::User do

  let(:view) do
    described_class.new(authorized_client.database)
  end

  describe '#create' do

    let!(:response) do
      view.create(
        'durran',
        'password',
        roles: [ Mongo::Auth::Roles::READ_WRITE ]
      )
    end

    after do
      view.remove('durran')
    end

    context 'when user creation was successful' do

      it 'saves the user in the database' do
        expect(response).to be_ok
      end
    end

    context 'when creation was not successful' do

      it 'raises an exception' do
        expect {
          view.create('durran', 'password')
        }.to raise_error(Mongo::Operation::Write::Failure)
      end
    end
  end

  describe '#remove' do

    context 'when user removal was successful' do

      before do
        view.create(
          'durran',
          'password',
          roles: [ Mongo::Auth::Roles::READ_WRITE ]
        )
      end

      let(:response) do
        view.remove('durran')
      end

      it 'saves the user in the database' do
        expect(response).to be_ok
      end
    end

    context 'when removal was not successful' do

      it 'raises an exception' do
        expect {
          view.remove('notauser')
        }.to raise_error(Mongo::Operation::Write::Failure)
      end
    end
  end
end
