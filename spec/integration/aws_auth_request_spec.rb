# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require 'net/http'

describe Mongo::Auth::Aws::Request do
  require_aws_auth

  before(:all) do
    if ENV['AUTH'] =~ /aws-(ec2|ecs|web)/
      skip "This test requires explicit credentials to be provided"
    end
  end

  let(:access_key_id) { ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID') }
  let(:secret_access_key) { ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY') }
  let(:session_token) { ENV['MONGO_RUBY_DRIVER_AWS_AUTH_SESSION_TOKEN'] }

  describe '#authorization' do
    let(:request) do
      described_class.new(
        access_key_id: access_key_id,
        secret_access_key: secret_access_key,
        session_token: session_token,
        host: 'sts.amazonaws.com',
        server_nonce: 'aaaaaaaaaaafake',
      )
    end

    let(:sts_request) do
      Net::HTTP::Post.new("https://sts.amazonaws.com").tap do |req|
        request.headers.each do |k, v|
          req[k] = v
        end
        req['authorization'] = request.authorization
        req['accept'] = 'application/json'
        req.body = described_class::STS_REQUEST_BODY
      end
    end

    let(:sts_response) do
      http = Net::HTTP.new('sts.amazonaws.com', 443)
      http.use_ssl = true

      # Uncomment to log complete request headers and the response.
      # WARNING: do not enable this in Evergreen as this can expose real
      # AWS credentias.
      #http.set_debug_output(STDERR)

      http.start do
        resp = http.request(sts_request)
      end
    end

    let(:sts_response_payload) do
      JSON.parse(sts_response.body)
    end

    let(:result) do
      sts_response_payload['GetCallerIdentityResponse']['GetCallerIdentityResult']
    end

    it 'is usable' do
      # This assertion intentionally does not use payload so that if it fails,
      # the entire response is printed for diagnostic purposes.
      sts_response.body.should_not =~ /"Error"/

      sts_response.code.should == '200'
      result['Arn'].should =~ /^arn:aws:(iam|sts)::/
      result['Account'].should be_a(String)
      result['UserId'].should =~ /^A/

      puts "STS request successful with ARN #{result['Arn']}"
    end
  end
end
