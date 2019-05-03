require 'spec_helper'

describe 'Command' do

  let(:subscriber) { EventSubscriber.new }

  describe 'payload' do
    let(:server) { authorized_client.cluster.next_primary }

    let(:payload) do
      command.send(:final_operation, server).send(:message, server).payload.dup.tap do |payload|
        if payload['request_id'].is_a?(Integer)
          payload['request_id'] = 42
        end
        # $clusterTime may be present depending on the client's state
        payload['command'].delete('$clusterTime')
        # 3.6+ servers also return a payload field, earlier ones do not.
        # The contents of this field duplicates the rest of the response
        # so we can get rid of it without losing information.
        payload.delete('reply')
      end
    end

    let(:session) { nil }

    context 'commitTransaction' do
      # Although these are unit tests, when targeting pre-4.0 servers
      # the driver does not add arguments like write concerns to commands that
      # it adds for 4.0+ servers, breaking expectations
      min_server_fcv '4.0'

      let(:selector) do
        { commitTransaction: 1 }.freeze
      end

      let(:write_concern) { nil }

      let(:command) do
        Mongo::Operation::Command.new(
          selector: selector,
          db_name: 'admin',
          session: session,
          txn_num: 123,
          write_concern: write_concern,
        )
      end

      let(:expected_payload) do
        {
          'command' => {'commitTransaction' => 1},
          'command_name' => 'commitTransaction',
          'database_name' => 'admin',
          'request_id' => 42,
        }
      end

      it 'returns expected payload' do
        expect(payload).to eq(expected_payload)
      end

      context 'with session' do
        min_server_fcv '3.6'

        let(:session) { authorized_client.start_session }

        let(:expected_payload) do
          {
            'command' => {
              'commitTransaction' => 1,
              'lsid' => session.session_id,
              'txnNumber' => BSON::Int64.new(123),
            },
            'command_name' => 'commitTransaction',
            'database_name' => 'admin',
            'request_id' => 42,
          }
        end

        it 'returns selector with write concern' do
          expect(payload).to eq(expected_payload)
        end
      end

      context 'with write concern' do
        let(:write_concern) { Mongo::WriteConcern.get(w: :majority) }

        let(:expected_payload) do
          {
            'command' => {
              'commitTransaction' => 1,
              'writeConcern' => {'w' => 'majority'},
            },
            'command_name' => 'commitTransaction',
            'database_name' => 'admin',
            'request_id' => 42,
          }
        end

        it 'returns selector with write concern' do
          expect(payload).to eq(expected_payload)
        end
      end
    end

    context 'find' do
      let(:selector) do
        { find: 'collection_name' }.freeze
      end

      let(:command) do
        Mongo::Operation::Command.new(
          selector: selector,
          db_name: 'foo',
          session: session,
        )
      end

      let(:expected_payload) do
        {
          'command' => {'find' => 'collection_name'},
          'command_name' => 'find',
          'database_name' => 'foo',
          'request_id' => 42,
        }
      end

      it 'returns expected payload' do
        expect(payload).to eq(expected_payload)
      end
    end

  end

end
