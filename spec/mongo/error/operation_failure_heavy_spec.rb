require 'spec_helper'

describe Mongo::Error::OperationFailure do

  describe '#write_concern_error' do
    # Fail point will work on 4.0 mongod but requires 4.2 for mongos
    min_server_fcv '4.2'
    # Fail point must be set on the same server to which the query is sent
    require_no_multi_shard

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
end
