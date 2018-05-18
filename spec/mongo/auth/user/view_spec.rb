require 'spec_helper'

describe Mongo::Auth::User::View do

  let(:view) do
    described_class.new(root_authorized_client.database)
  end

  after do
    begin; view.remove('durran'); rescue; end
  end

  describe '#create' do

    context 'when a session is not used' do

      let!(:response) do
        view.create(
          'durran',
          {
            password: 'password',
            roles: [Mongo::Auth::Roles::READ_WRITE],
          }
        )
      end

      context 'when user creation was successful' do

        it 'saves the user in the database' do
          expect(response).to be_successful
        end

        context 'when compression is used', if: testing_compression? do

          it 'does not compress the message' do
            # The dropUser command message will be compressed, so expect instantiation once.
            expect(Mongo::Protocol::Compressed).to receive(:new).once.and_call_original
            expect(response).to be_successful
          end
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

    context 'when a session is used' do

      let(:operation) do
        view.create(
            'durran',
            password: 'password',
            roles: [Mongo::Auth::Roles::READ_WRITE],
            session: session
        )
      end

      let(:session) do
        client.start_session
      end

      let(:client) do
        root_authorized_client
      end

      it_behaves_like 'an operation using a session'
    end
  end

  describe '#update' do

    before do
      view.create(
          'durran',
          password: 'password', roles: [Mongo::Auth::Roles::READ_WRITE]
      )
    end

    context 'when a user password is updated' do

      context 'when a session is not used' do

        let!(:response) do
          view.update(
              'durran',
              password: '123', roles: [ Mongo::Auth::Roles::READ_WRITE ]
          )
        end

        it 'updates the password' do
          expect(response).to be_successful
        end

        context 'when compression is used', if: testing_compression? do

          it 'does not compress the message' do
            # The dropUser command message will be compressed, so expect instantiation once.
            expect(Mongo::Protocol::Compressed).to receive(:new).once.and_call_original
            expect(response).to be_successful
          end
        end
      end

      context 'when a session is used' do

        let(:operation) do
          view.update(
              'durran',
              password: '123',
              roles: [ Mongo::Auth::Roles::READ_WRITE ],
              session: session
          )
        end

        let(:session) do
          client.start_session
        end

        let(:client) do
          root_authorized_client
        end

        it_behaves_like 'an operation using a session'
      end
    end

    context 'when the roles of a user are updated' do

      context 'when a session is not used' do

        let!(:response) do
          view.update(
              'durran',
              password: 'password', roles: [ Mongo::Auth::Roles::READ ]
          )
        end

        it 'updates the roles' do
          expect(response).to be_successful
        end

        context 'when compression is used', if: testing_compression? do

          it 'does not compress the message' do
            # The dropUser command message will be compressed, so expect instantiation once.
            expect(Mongo::Protocol::Compressed).to receive(:new).once.and_call_original
            expect(response).to be_successful
          end
        end
      end

      context 'when a session is used' do

        let(:operation) do
          view.update(
              'durran',
              password: 'password',
              roles: [ Mongo::Auth::Roles::READ ],
              session: session
          )
        end

        let(:session) do
          client.start_session
        end

        let(:client) do
          root_authorized_client
        end

        it_behaves_like 'an operation using a session'
      end
    end
  end

  describe '#remove' do

    context 'when a session is not used' do

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

        it 'raises an exception' do
          expect {
            view.remove('notauser')
          }.to raise_error(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when a session is used' do

      context 'when user removal was successful' do

        before do
          view.create(
              'durran',
              password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
          )
        end

        let(:operation) do
          view.remove('durran', session: session)
        end

        let(:session) do
          client.start_session
        end

        let(:client) do
          root_authorized_client
        end

        it_behaves_like 'an operation using a session'
      end

      context 'when removal was not successful' do

        let(:failed_operation) do
          view.remove('notauser', session: session)
        end

        let(:session) do
          client.start_session
        end

        let(:client) do
          root_authorized_client
        end

        it_behaves_like 'a failed operation using a session'
      end
    end
  end

  describe '#info' do

    context 'when a session is not used' do

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

      context 'when a user is not authorized' do

        let(:view) do
          described_class.new(unauthorized_client.database)
        end

        it 'raises an OperationFailure', if: auth_enabled? do
          expect{
            view.info('emily')
          }.to raise_exception(Mongo::Error::OperationFailure)
        end
      end
    end

    context 'when a session is used' do

      context 'when a user exists in the database' do

        before do
          view.create(
              'durran',
              password: 'password'
          )
        end

        let(:operation) do
          view.info('durran', session: session)
        end

        let(:session) do
          client.start_session
        end

        let(:client) do
          root_authorized_client
        end

        it_behaves_like 'an operation using a session'
      end

      context 'when a user does not exist in the database' do

        let(:operation) do
          view.info('emily', session: session)
        end

        let(:session) do
          client.start_session
        end

        let(:client) do
          root_authorized_client
        end

        it_behaves_like 'an operation using a session'
      end
    end
  end
end
