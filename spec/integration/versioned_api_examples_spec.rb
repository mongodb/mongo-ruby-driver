# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'Versioned API examples' do

  # Until https://jira.mongodb.org/browse/RUBY-1768 is implemented, limit
  # the tests to simple configurations
  require_no_auth
  require_no_tls
  min_server_version("5.0")

  let(:uri_string) do
    "mongodb://#{SpecConfig.instance.addresses.join(',')}/versioned-api-examples"
  end

  it 'Versioned API example 1' do

    # Start Versioned API Example 1

    client = Mongo::Client.new(uri_string, server_api: {version: "1"})

    # End Versioned API Example 1

    # Run a command to ensure the client works.
    client['test'].find.to_a.should be_a(Array)
    # Do not leak clients.
    client.close
  end

  it 'Versioned API example 2' do
    # Start Versioned API Example 2

    client = Mongo::Client.new(uri_string, server_api: {version: "1", strict: true})

    # End Versioned API Example 2

    # Run a command to ensure the client works.
    client['test'].find.to_a.should be_a(Array)
    # Do not leak clients.
    client.close
  end

  it 'Versioned API example 3' do
    # Start Versioned API Example 3

    client = Mongo::Client.new(uri_string, server_api: {version: "1", strict: false})

    # End Versioned API Example 3

    # Run a command to ensure the client works.
    client['test'].find.to_a.should be_a(Array)
    # Do not leak clients.
    client.close
  end

  it 'Versioned API example 4' do
    # Start Versioned API Example 4

    client = Mongo::Client.new(uri_string, server_api: {version: "1", deprecation_errors: true})

    # End Versioned API Example 4

    # Run a command to ensure the client works.
    client['test'].find.to_a.should be_a(Array)
    # Do not leak clients.
    client.close
  end

  # See also RUBY-2922 for count in versioned api v1.
  context 'servers that exclude count from versioned api' do
    max_server_version '5.0.8'

    it "Versioned API Strict Migration Example" do
      client = Mongo::Client.new(uri_string, server_api: {version: "1", strict: true})
      client[:sales].drop

      # Start Versioned API Example 5

      client[:sales].insert_many([
        { _id: 1, item: "abc", price: 10, quantity: 2,  date: DateTime.parse("2021-01-01T08:00:00Z") },
        { _id: 2, item: "jkl", price: 20, quantity: 1,  date: DateTime.parse("2021-02-03T09:00:00Z") },
        { _id: 3, item: "xyz", price: 5,  quantity: 5,  date: DateTime.parse("2021-02-03T09:05:00Z") },
        { _id: 4, item: "abc", price: 10, quantity: 10, date: DateTime.parse("2021-02-15T08:00:00Z") },
        { _id: 5, item: "xyz", price: 5,  quantity: 10, date: DateTime.parse("2021-02-15T09:05:00Z") },
        { _id: 6, item: "xyz", price: 5,  quantity: 5,  date: DateTime.parse("2021-02-15T12:05:10Z") },
        { _id: 7, item: "xyz", price: 5,  quantity: 10, date: DateTime.parse("2021-02-15T14:12:12Z") },
        { _id: 8, item: "abc", price: 10, quantity: 5,  date: DateTime.parse("2021-03-16T20:20:13Z") }
      ])

      # End Versioned API Example 5

      expect do
        client.database.command(count: :sales)
      end.to raise_error(Mongo::Error::OperationFailure)

      # Start Versioned API Example 6

      # Mongo::Error::OperationFailure:
      #   [323:APIStrictError]: Provided apiStrict:true, but the command count is not in API Version 1. Information on supported commands and migrations in API Version 1 can be found at https://www.mongodb.com/docs/manual/reference/stable-api

      # End Versioned API Example 6

      # Start Versioned API Example 7

      client[:sales].count_documents

      # End Versioned API Example 7

      # Start Versioned API Example 8

      # 8

      # End Versioned API Example 8
      # Do not leak clients.
      client.close
    end
  end
end
