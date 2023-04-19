# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

AWS_REGION_TEST_CASES = {
  'sts.amazonaws.com' => 'us-east-1',
  'sts.us-west-2.amazonaws.com' => 'us-west-2',
  'sts.us-west-2.amazonaws.com.ch' => 'us-west-2',
  'example.com' => 'com',
  'localhost' => 'us-east-1',
  'sts..com' => Mongo::Error::InvalidServerAuthHost,
  '.amazonaws.com' => Mongo::Error::InvalidServerAuthHost,
  'sts.amazonaws.' => Mongo::Error::InvalidServerAuthHost,
  '' => Mongo::Error::InvalidServerAuthResponse,
  'x' * 256 => Mongo::Error::InvalidServerAuthHost,
}

describe 'AWS auth region tests' do

  AWS_REGION_TEST_CASES.each do |host, expected_region|
    context "host '#{host}'" do
      let(:request) do
        Mongo::Auth::Aws::Request.new(access_key_id: 'access_key_id',
          secret_access_key: 'secret_access_key',
          session_token: 'session_token',
          host: host,
          server_nonce: 'server_nonce',
        )
      end

      if expected_region.is_a?(String)
        it 'derives expected region' do
          request.region.should == expected_region
        end
      else
        it 'fails with an error' do
          lambda do
            request.region
          end.should raise_error(expected_region)
        end
      end
    end
  end
end
