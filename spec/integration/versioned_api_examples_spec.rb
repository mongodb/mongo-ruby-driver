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

  it 'Versioned API examples' do

    # Start Versioned API Example 1

    client = Mongo::Client.new(uri_string, server_api: {version: "1"})

    # End Versioned API Example 1

    # Start Versioned API Example 2

    client = Mongo::Client.new(uri_string, server_api: {version: "1", strict: true})

    # End Versioned API Example 2

    # Start Versioned API Example 3

    client = Mongo::Client.new(uri_string, server_api: {version: "1", strict: false})

    # End Versioned API Example 3

    # Start Versioned API Example 4

    client = Mongo::Client.new(uri_string, server_api: {version: "1", deprecation_errors: true})

    # End Versioned API Example 4
  end
end

