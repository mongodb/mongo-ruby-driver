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

  'Invalid - AWS_EXECUTION_ENV does not start with AWS_Lambda_' => {
    'AWS_EXECUTION_ENV' => 'EC2',
  },

  'Valid container and FaaS provider' => {
    'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
    'AWS_REGION' => 'us-east-2',
    'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024',
    'KUBERNETES_SERVICE_HOST' => '1',
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

  # Test 1 case 9 requires verifying that both the container metadata and the
  # AWS Lambda metadata are present in client.env, not just that the connection
  # succeeds.
  context 'when given a container and a FaaS provider' do
    local_env(
      'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
      'AWS_REGION' => 'us-east-2',
      'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024',
      'KUBERNETES_SERVICE_HOST' => '1'
    )

    it 'includes both container and AWS Lambda metadata in client.env' do
      recorder = record_handshake_documents

      authorized_client.database.command(ping: 1)

      env = recorder.documents.filter_map { |document| document[:client][:env] }.first
      expect(env).not_to be_nil
      expect(env[:name]).to eq('aws.lambda')
      expect(env[:container]).to include(orchestrator: 'kubernetes')
    end
  end
end
