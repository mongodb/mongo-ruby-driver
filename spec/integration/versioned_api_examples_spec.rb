# frozen_string_literal: true
# encoding: utf-8

require 'spec_helper'

describe 'Versioned API examples' do

  # Until https://jira.mongodb.org/browse/RUBY-1768 is implemented, limit
  # the tests to simple configurations
  require_no_auth
  require_no_tls

  let(:uri_string) do
    "mongodb://#{SpecConfig.instance.addresses.join(',')}"
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
end
