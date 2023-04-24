# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Transactions API examples' do
  require_wired_tiger
  require_transaction_support

  # Until https://jira.mongodb.org/browse/RUBY-1768 is implemented, limit
  # the tests to simple configurations
  require_no_auth
  require_no_tls

  let(:uri_string) do
    "mongodb://#{SpecConfig.instance.addresses.join(',')}"
  end

  it 'with_transaction API example 1' do

    # Start Transactions withTxn API Example 1

    # For a replica set, include the replica set name and a seedlist of the members in the URI string; e.g.
    # uriString = 'mongodb://mongodb0.example.com:27017,mongodb1.example.com:27017/?replicaSet=myRepl'
    # For a sharded cluster, connect to the mongos instances; e.g.
    # uri_string = 'mongodb://mongos0.example.com:27017,mongos1.example.com:27017/'

    client = Mongo::Client.new(uri_string, write_concern: {w: :majority, wtimeout: 1000})

    # Prereq: Create collections.

    client.use('mydb1')['foo'].insert_one(abc: 0)
    client.use('mydb2')['bar'].insert_one(xyz: 0)

    # Step 1: Define the callback that specifies the sequence of operations to perform inside the transactions.

    callback = Proc.new do |my_session|
      collection_one = client.use('mydb1')['foo']
      collection_two = client.use('mydb2')['bar']

      # Important: You must pass the session to the operations.

      collection_one.insert_one({'abc': 1}, session: my_session)
      collection_two.insert_one({'xyz': 999}, session: my_session)
    end

    #. Step 2: Start a client session.

    session = client.start_session

    # Step 3: Use with_transaction to start a transaction, execute the callback, and commit (or abort on error).

    session.with_transaction(
      read_concern: {level: :local},
      write_concern: {w: :majority, wtimeout: 1000},
      read: {mode: :primary},
      &callback)

    # End Transactions withTxn API Example 1

  end
end
