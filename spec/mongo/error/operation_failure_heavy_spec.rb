# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Error::OperationFailure do

  describe '#write_concern_error' do
    # Fail point will work on 4.0 mongod but requires 4.2 for mongos
    min_server_fcv '4.2'
    # Fail point must be set on the same server to which the query is sent
    require_no_multi_mongos

    # https://github.com/mongodb/specifications/commit/7745234f93039a83ae42589a6c0cdbefcffa32fa
    let(:fail_point_command) do
     {
       "configureFailPoint": "failCommand",
       "data": {
         "failCommands": ["insert"],
         "writeConcernError": {
           "code": 100,
           "codeName": "UnsatisfiableWriteConcern",
           "errmsg": "Not enough data-bearing nodes",
           "errInfo": {
             "writeConcern": {
               "w": 2,
               "wtimeout": 0,
               "provenance": "clientSupplied"
             }
           }
         }
       },
       "mode": { "times": 1 }
     }
    end

    it 'exposes all server-provided fields' do
      authorized_client.use('admin').command(fail_point_command)

      begin
        authorized_client['foo'].insert_one(test: 1)
      rescue Mongo::Error::OperationFailure => exc
        expect(exc.details).to eq(exc.document['writeConcernError']['errInfo'])
        expect(exc.server_message).to eq(exc.document['writeConcernError']['errmsg'])
        expect(exc.code).to eq(exc.document['writeConcernError']['code'])
      else
        fail 'Expected an OperationFailure'
      end

      exc.write_concern_error_document.should == {
        'code' => 100,
        'codeName' => 'UnsatisfiableWriteConcern',
        'errmsg' => 'Not enough data-bearing nodes',
        'errInfo' => {
          'writeConcern' => {
            'w' => 2,
            'wtimeout' => 0,
            'provenance' => 'clientSupplied',
          },
        },
      }
    end
  end

  describe 'WriteError details' do
    min_server_fcv '5.0'

    let(:subscriber) { Mrss::EventSubscriber.new }

    let(:subscribed_client) do
      authorized_client.tap do |client|
        client.subscribe(Mongo::Monitoring::COMMAND, subscriber)
      end
    end

    let(:collection_name) { 'write_error_prose_spec' }

    let(:collection) do
      subscribed_client[:collection_name].drop
      subscribed_client[:collection_name,
      {
        'validator' => {
          'x' => { '$type' => 'string' },
        }
      }].create
      subscribed_client[:collection_name]
    end

    context 'when there is a write error' do
      it 'succeeds and prints the error' do
        begin
          collection.insert_one({x: 1})
        rescue Mongo::Error::OperationFailure => e
          insert_events = subscriber.succeeded_events.select { |e| e.command_name == "insert" }
          expect(insert_events.length).to eq 1
          expect(e.message).to match(/\[#{e.code}(:.*)?\].+ -- .+/)

          expect(e.details).to eq(e.document['writeErrors'][0]['errInfo'])
          expect(e.server_message).to eq(e.document['writeErrors'][0]['errmsg'])
          expect(e.code).to eq(e.document['writeErrors'][0]['code'])

          expect(e.code).to eq 121
          expect(e.details).to eq(insert_events[0].reply['writeErrors'][0]['errInfo'])
        else
          fail 'Expected an OperationFailure'
        end
      end
    end
  end
end
