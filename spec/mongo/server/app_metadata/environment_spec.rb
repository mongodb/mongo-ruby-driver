# frozen_string_literal: true

require 'spec_helper'

describe Mongo::Server::AppMetadata::Environment do
  let(:env) { described_class.new }

  shared_examples_for 'running in a FaaS environment' do
    it 'reports that a FaaS environment is detected' do
      expect(env.faas?).to be true
    end
  end

  shared_examples_for 'running outside a FaaS environment' do
    it 'reports that no FaaS environment is detected' do
      expect(env.faas?).to be false
    end
  end

  context 'when run outside of a FaaS environment' do
    it_behaves_like 'running outside a FaaS environment'
  end

  context 'when run in a FaaS environment' do
    context 'when environment is invalid due to type mismatch' do
      local_env(
        'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
        'AWS_REGION' => 'us-east-2',
        'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => 'big'
      )

      it_behaves_like 'running outside a FaaS environment'

      it 'fails due to type mismatch' do
        expect(env.error).to match(/AWS_LAMBDA_FUNCTION_MEMORY_SIZE must be integer/)
      end
    end

    context 'when environment is invalid due to long string' do
      local_env(
        'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
        'AWS_REGION' => 'a' * 512,
        'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024'
      )

      it_behaves_like 'running outside a FaaS environment'

      it 'fails due to long string' do
        expect(env.error).to match(/too long/)
      end
    end

    context 'when environment is invalid due to multiple providers' do
      local_env(
        'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
        'AWS_REGION' => 'us-east-2',
        'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024',
        'FUNCTIONS_WORKER_RUNTIME' => 'ruby'
      )

      it_behaves_like 'running outside a FaaS environment'

      it 'fails due to multiple providers' do
        expect(env.error).to match(/too many environments/)
      end
    end

    context 'when VERCEL and AWS are both given' do
      local_env(
        'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
        'AWS_REGION' => 'us-east-2',
        'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024',
        'VERCEL' => '1',
        'VERCEL_REGION' => 'cdg1'
      )

      it_behaves_like 'running in a FaaS environment'

      it 'prefers vercel' do
        expect(env.aws?).to be false
        expect(env.vercel?).to be true
        expect(env.fields[:region]).to be == 'cdg1'
      end
    end

    context 'when environment is invalid due to missing variable' do
      local_env(
        'AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7',
        'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024'
      )

      it_behaves_like 'running outside a FaaS environment'

      it 'fails due to missing variable' do
        expect(env.error).to match(/missing environment variable/)
      end
    end

    context 'when FaaS environment is AWS' do
      shared_examples_for 'running in an AWS environment' do
        context 'when environment is valid' do
          local_env(
            'AWS_REGION' => 'us-east-2',
            'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024'
          )

          it_behaves_like 'running in a FaaS environment'

          it 'recognizes AWS' do
            expect(env.name).to be == 'aws.lambda'
            expect(env.fields[:region]).to be == 'us-east-2'
            expect(env.fields[:memory_mb]).to be == 1024
          end
        end
      end

      # per DRIVERS-2623, AWS_EXECUTION_ENV must be prefixed
      # with 'AWS_Lambda_'.
      context 'when AWS_EXECUTION_ENV is invalid' do
        local_env(
          'AWS_EXECUTION_ENV' => 'EC2',
          'AWS_REGION' => 'us-east-2',
          'AWS_LAMBDA_FUNCTION_MEMORY_SIZE' => '1024'
        )

        it_behaves_like 'running outside a FaaS environment'
      end

      context 'when AWS_EXECUTION_ENV is detected' do
        local_env('AWS_EXECUTION_ENV' => 'AWS_Lambda_ruby2.7')
        it_behaves_like 'running in an AWS environment'
      end

      context 'when AWS_LAMBDA_RUNTIME_API is detected' do
        local_env('AWS_LAMBDA_RUNTIME_API' => 'lambda.aws.amazon.com/api')
        it_behaves_like 'running in an AWS environment'
      end
    end

    context 'when FaaS environment is Azure' do
      local_env('FUNCTIONS_WORKER_RUNTIME' => 'ruby')

      it_behaves_like 'running in a FaaS environment'

      it 'recognizes Azure' do
        expect(env.name).to be == 'azure.func'
      end
    end

    context 'when FaaS environment is GCP' do
      local_env(
        'FUNCTION_MEMORY_MB' => '1024',
        'FUNCTION_TIMEOUT_SEC' => '60',
        'FUNCTION_REGION' => 'us-central1'
      )

      shared_examples_for 'running in a GCP environment' do
        it_behaves_like 'running in a FaaS environment'

        it 'recognizes GCP' do
          expect(env.name).to be == 'gcp.func'
          expect(env.fields[:region]).to be == 'us-central1'
          expect(env.fields[:memory_mb]).to be == 1024
          expect(env.fields[:timeout_sec]).to be == 60
        end
      end

      context 'when K_SERVICE is present' do
        local_env('K_SERVICE' => 'servicename')
        it_behaves_like 'running in a GCP environment'
      end

      context 'when FUNCTION_NAME is present' do
        local_env('FUNCTION_NAME' => 'functionName')
        it_behaves_like 'running in a GCP environment'
      end
    end

    context 'when FaaS environment is Vercel' do
      local_env(
        'VERCEL' => '1',
        'VERCEL_REGION' => 'cdg1'
      )

      it_behaves_like 'running in a FaaS environment'

      it 'recognizes Vercel' do
        expect(env.name).to be == 'vercel'
        expect(env.fields[:region]).to be == 'cdg1'
      end
    end

    context 'when converting environment to a hash' do
      local_env(
        'K_SERVICE' => 'servicename',
        'FUNCTION_MEMORY_MB' => '1024',
        'FUNCTION_TIMEOUT_SEC' => '60',
        'FUNCTION_REGION' => 'us-central1'
      )

      it 'includes name and all fields' do
        expect(env.to_h).to be == {
          name: 'gcp.func', memory_mb: 1024,
          timeout_sec: 60, region: 'us-central1',
        }
      end
    end
  end
end
