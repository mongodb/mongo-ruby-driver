# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::User::View do

  let(:database) { root_authorized_client.database }

  let(:view) do
    described_class.new(database)
  end

  before do
    # Separate view instance to not interfere with test assertions
    view = described_class.new(root_authorized_client.database)
    begin
      view.remove('durran')
    rescue Mongo::Error::OperationFailure
    end
  end

  shared_context 'testing write concern' do

    let(:subscriber) do
      Mrss::EventSubscriber.new
    end

    let(:client) do
      root_authorized_client.tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    let(:view) do
      described_class.new(client.database)
    end

    before do
      allow_any_instance_of(Mongo::Monitoring::Event::CommandStarted).to receive(:redacted) do |instance, command_name, document|
        document
      end
    end
  end

  shared_examples_for 'forwards write concern to server' do
    # w:2 requires more than one node in the deployment
    require_topology :replica_set

    it 'forwards write concern to server' do
      response

      expect(event.command['writeConcern']).to eq('w' => 2)
    end
  end

  describe '#create' do

    context 'when password is not provided' do

      let(:database) { root_authorized_client.use('$external').database }

      let(:username) { 'passwordless-user' }

      let(:response) do
        view.create(
          username,
          # https://stackoverflow.com/questions/55939832/mongodb-external-database-cannot-create-new-user-with-user-defined-role
          roles: [{role: 'read', db: 'admin'}],
        )
      end

      before do
        begin
          view.remove(username)
        rescue Mongo::Error::OperationFailure
          # can be user not found, ignore
        end
      end

      it 'creates the user' do
        view.info(username).should == []

        lambda do
          response
        end.should_not raise_error

        view.info(username).first['user'].should == username
      end
    end

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

        context 'when compression is used' do
          require_compression
          min_server_fcv '3.6'

          it 'does not compress the message' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
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

    context 'when write concern is given' do
      include_context 'testing write concern'

      let(:response) do
        view.create(
          'durran',
          password: 'password',
          roles: [Mongo::Auth::Roles::READ_WRITE],
          write_concern: {w: 2},
        )
      end

      let(:event) do
        subscriber.single_command_started_event('createUser')
      end

      it_behaves_like 'forwards write concern to server'
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

        context 'when compression is used' do
          require_compression
          min_server_fcv '3.6'

          it 'does not compress the message' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
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

        context 'when compression is used' do
          require_compression
          min_server_fcv '3.6'

          it 'does not compress the message' do
            expect(Mongo::Protocol::Compressed).not_to receive(:new)
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

    context 'when write concern is given' do
      include_context 'testing write concern'

      let(:response) do
        view.update(
          'durran',
          password: 'password1',
          roles: [Mongo::Auth::Roles::READ_WRITE],
          write_concern: {w: 2},
        )
      end

      let(:event) do
        subscriber.single_command_started_event('updateUser')
      end

      it_behaves_like 'forwards write concern to server'
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

    context 'when write concern is given' do
      include_context 'testing write concern'

      before do
        view.create(
            'durran',
            password: 'password', roles: [ Mongo::Auth::Roles::READ_WRITE ]
        )
      end

      let(:response) do
        view.remove(
          'durran',
          write_concern: {w: 2},
        )
      end

      let(:event) do
        subscriber.single_command_started_event('dropUser')
      end

      it_behaves_like 'forwards write concern to server'
    end
  end

  describe '#info' do

    context 'when a session is not used' do

      before do
        view.remove('emily') rescue nil
      end

      context 'when a user exists in the database' do

        before do
          view.create(
              'emily',
              password: 'password'
          )
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
        require_auth

        let(:view) do
          described_class.new(unauthorized_client.database)
        end

        it 'raises an OperationFailure' do
          expect do
            view.info('emily')
          end.to raise_exception(Mongo::Error::OperationFailure)
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

  context "when the result is a write concern error" do
    require_topology :replica_set
    min_server_version '4.0'

    let(:user) do
      Mongo::Auth::User.new({
        user: 'user',
        roles: [ Mongo::Auth::Roles::READ_WRITE ],
        password: 'password'
      })
    end

    before do
      authorized_client.use('admin').database.command(
        configureFailPoint: "failCommand",
        mode: { times: 1 },
        data: {
          failCommands: [ failCommand ],
          writeConcernError: {
              code: 64,
              codeName: "WriteConcernFailed",
              errmsg: "waiting for replication timed out",
              errInfo: { wtimeout: true }
          }
        }
      )
    end

    shared_examples "raises the correct write concern error" do

      it "raises a write concern error" do
        expect do
          view.send(method, input)
        end.to raise_error(Mongo::Error::OperationFailure, /[64:WriteConcernFailed]/)
      end

      it "raises and reports the write concern error correctly" do
        begin
          view.send(method, input)
        rescue Mongo::Error::OperationFailure => e
          expect(e.write_concern_error?).to be true
          expect(e.write_concern_error_document).to eq(
            "code" => 64,
            "codeName" => "WriteConcernFailed",
            "errmsg" => "waiting for replication timed out",
            "errInfo" => { "wtimeout" => true }
          )
        end
      end
    end

    context "when creating a user" do

      let(:failCommand) { "createUser" }
      let(:method) { :create }
      let(:input) { user }

      after do
        view.remove(user.name)
      end

      include_examples "raises the correct write concern error"
    end

    context "when updating a user" do

      let(:failCommand) { "updateUser" }
      let(:method) { :update }
      let(:input) { user.name }

      before do
        view.create(user)
      end

      after do
        view.remove(user.name)
      end

      include_examples "raises the correct write concern error"
    end

    context "when removing a user" do

      let(:failCommand) { "dropUser" }
      let(:method) { :remove }
      let(:input) { user.name }

      before do
        view.create(user)
      end

      include_examples "raises the correct write concern error"
    end
  end
end
