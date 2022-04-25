# frozen_string_literal: true
# encoding: utf-8

require "spec_helper"

describe "AWS Lambda examples in Ruby" do
  require_aws_auth

  it "shares the client" do
    # Start AWS Lambda Example 1

    # Require the driver library.
    require "mongo"

    # Create a Mongo::Client instance.
    # CRITICAL: You must create the client instance outside the handler
    # so that the client can be reused across function invocations.
    client = Mongo::Client.new(ENV.fetch("MONGODB_URI"))

    def lambda_handler(event:, context:)
      # Use the client.
      client['events'].insert_one(event: event)
    end

    # End AWS Lambda Example 1

    client.close
  end

  it "to the deployment using AWS IAM authentication" do
    allow(ENV).to receive(:[]).and_call_original
    allow(ENV).to receive(:[]).with("MONGODB_HOST").and_return(SpecConfig.instance.addresses.first)
    allow(ENV).to receive(:[]).with("AWS_ACCESS_KEY_ID").and_return(ENV.fetch("MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID"))
    allow(ENV).to receive(:[]).with("AWS_SECRET_ACCESS_KEY").and_return(ENV.fetch("MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY"))
    allow(ENV).to receive(:[]).with("AWS_SESSION_TOKEN").and_return(ENV.fetch("MONGO_RUBY_DRIVER_AWS_AUTH_SESSION_TOKEN"))
    allow(ENV).to receive(:[]).with("MONGODB_DATABASE").and_return("test")

    # Start AWS Lambda Example 2

    # Require the driver library.
    require "mongo"

    # Create a Mongo::Client instance using AWS IAM authentication.
    # CRITICAL: You must create the client instance outside the handler
    # so that the client can be reused across function invocations.
    client = Mongo::Client.new([ENV.fetch("MONGODB_HOST"]),
                               auth_mech: :aws,
                               user: ENV.fetch("AWS_ACCESS_KEY_ID"),
                               password: ENV.fetch("AWS_SECRET_ACCESS_KEY"),
                               auth_mech_properties: {
                                 aws_session_token: ENV.fetch("AWS_SESSION_TOKEN"),
                               },
                               database: ENV.fetch("MONGODB_DATABASE"))

    def lambda_handler(event:, context:)
      # Use the client.
      client['events'].insert_one(event: event)
    end

    # End AWS Lambda Example 2

    client.close
  end
end
