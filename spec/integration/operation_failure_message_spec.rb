# rubocop:todo all
require 'spec_helper'

describe 'OperationFailure message' do
  let(:client) { authorized_client }
  let(:collection_name) { 'operation_failure_message_spec' }
  let(:collection) { client[collection_name] }

  context 'crud error' do
    before do
      collection.delete_many
    end

    context 'a command error with code and code name' do
      context 'on modern servers that provide code name' do
        # Sharded clusters include the code name: SERVER-55582
        require_topology :single, :replica_set

        min_server_fcv '3.4'

        it 'reports code, code name and message' do
          begin
            client.command(bogus_command: nil)
            fail('Should have raised')
          rescue Mongo::Error::OperationFailure => e
            e.code_name.should == 'CommandNotFound'
            e.message.should =~ %r,\A\[59:CommandNotFound\]: no such (?:command|cmd): '?bogus_command'?,
          end
        end
      end

      context 'on legacy servers where code name is not provided' do
        max_server_version '3.2'

        it 'reports code and message' do
          begin
            client.command(bogus_command: nil)
            fail('Should have raised')
          rescue Mongo::Error::OperationFailure => e
            e.code_name.should be nil
            e.message.should =~ %r,\A\[59\]: no such (?:command|cmd): '?bogus_command'?,
          end
        end
      end
    end

    context 'a write error with code and no code name' do
      # Sharded clusters include the code name: SERVER-55582
      require_topology :single, :replica_set

      it 'reports code name, code and message' do
        begin
          collection.insert_one(_id: 1)
          collection.insert_one(_id: 1)
          fail('Should have raised')
        rescue Mongo::Error::OperationFailure => e
          e.code_name.should be nil
          e.message.should =~ %r,\A\[11000\]: (?:insertDocument :: caused by :: 11000 )?E11000 duplicate key error (?:collection|index):,
        end
      end
    end
  end

  context 'authentication error' do
    require_no_external_user

    let(:client) do
      authorized_client.with(user: 'bogus', password: 'bogus')
    end

    context 'on modern servers where code name is provided' do
      min_server_fcv '3.4'

      it 'includes code and code name in the message' do
        lambda do
          client.command(ping: 1)
        end.should raise_error(Mongo::Auth::Unauthorized, /User bogus.*is not authorized.*\[18:AuthenticationFailed\]: Authentication failed/)
      end
    end

    context 'on legacy servers where code name is not provided' do
      max_server_version '3.2'

      it 'includes code only in the message' do
        lambda do
          client.command(ping: 1)
        end.should raise_error(Mongo::Auth::Unauthorized, /User bogus.*is not authorized.*\[18\]: (?:Authentication|auth) failed/)
      end
    end
  end
end
