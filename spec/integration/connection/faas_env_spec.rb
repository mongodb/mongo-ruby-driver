# frozen_string_literal: true

require 'spec_helper'

# Test Plan scenarios from the handshake spec
SCENARIOS = {
  'Valid AWS' => {
    'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
    'AWS_REGION' => 'us-east-2',
    'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024',
  },

  'Valid Azure' => {
    'FUNCTIONS_WORKER_RUNTIME' => 'ruby',
  },

  'Valid GCP' => {
    'K_SERVICE' => 'servicename',
    'FUNCTION_MEMORY_MB' => '1024',
    'FUNCTION_TIMEOUT_SEC' => '60',
    'FUNCTION_REGION' => 'us-central1',
  },

  'Valid Vercel' => {
    'VERCEL' => '1',
    'VERCEL_REGION' => 'cdg1',
  },

  'Invalid - multiple providers' => {
    'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
    'AWS_REGION' => 'us-east-2',
    'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024',
    'FUNCTIONS_WORKER_RUNTIME' => 'ruby',
  },

  'Invalid - long string' => {
    'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
    'AWS_REGION' => 'a' * 512,
    'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024',
  },

  'Invalid - wrong types' => {
    'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
    'AWS_REGION' => 'us-east-2',
    'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => 'big',
  },
}.freeze

describe 'Connect under FaaS Env' do
  clean_slate

  SCENARIOS.each do |name, env|
    context "when given #{name}" do
      local_env(env)

      it 'connects successfully' do
        resp = authorized_client.database.command(ping: 1)
        expect(resp).to be_a(Mongo::Operation::Result)
      end
    end
  end
end
