# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'
require 'support/aws_utils'

describe Mongo::Auth::Aws::CredentialsRetriever do
  require_aws_auth

  let(:retriever) do
    described_class.new(user)
  end

  let(:credentials) do
    retriever.credentials
  end

  context 'when user is not given' do
    let(:user) do
      Mongo::Auth::User.new(auth_mech: :aws)
    end

    before do
      Mongo::Auth::Aws::CredentialsCache.instance.clear
    end

    shared_examples_for 'retrieves the credentials' do
      it 'retrieves' do
        credentials.should be_a(Mongo::Auth::Aws::Credentials)

        # When user is not given, credentials retrieved are always temporary.
        retriever.credentials.access_key_id.should =~ /^ASIA/
        retriever.credentials.secret_access_key.should =~ /./
        retriever.credentials.session_token.should =~ /./
      end

      let(:request) do
        Mongo::Auth::Aws::Request.new(
          access_key_id: credentials.access_key_id,
          secret_access_key: credentials.secret_access_key,
          session_token: credentials.session_token,
          host: 'sts.amazonaws.com',
          server_nonce: 'test',
        )
      end

      it 'produces valid credentials' do
        result = request.validate!
        puts "STS request successful with ARN #{result['Arn']}"
      end
    end

    context 'ec2 instance role' do
      require_ec2_host

      before(:all) do
        unless ENV['AUTH'] == 'aws-ec2'
          skip "Set AUTH=aws-ec2 in environment to run EC2 instance role tests"
        end
      end

      context 'when instance profile is not assigned' do
        before(:all) do
          orchestrator = AwsUtils::Orchestrator.new(
            region: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_REGION'),
            access_key_id: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
            secret_access_key: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
          )

          orchestrator.clear_instance_profile(Utils.ec2_instance_id)
          Utils.wait_for_no_instance_profile
        end

        it 'raises an error' do
          lambda do
            credentials
          end.should raise_error(Mongo::Auth::Aws::CredentialsNotFound, /Could not locate AWS credentials/)
        end
      end

      context 'when instance profile is assigned' do
        before(:all) do
          orchestrator = AwsUtils::Orchestrator.new(
            region: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_REGION'),
            access_key_id: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
            secret_access_key: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
          )

          orchestrator.set_instance_profile(Utils.ec2_instance_id,
            instance_profile_name: nil,
            instance_profile_arn: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_INSTANCE_PROFILE_ARN'),
          )
          Utils.wait_for_instance_profile
        end

        it_behaves_like 'retrieves the credentials'
      end
    end

    context 'ecs task role' do
      before(:all) do
        unless ENV['AUTH'] == 'aws-ecs'
          skip "Set AUTH=aws-ecs in environment to run ECS task role tests"
        end
      end

      it_behaves_like 'retrieves the credentials'
    end

    context 'web identity' do
      before(:all) do
        unless ENV['AUTH'] == 'aws-web-identity'
          skip "Set AUTH=aws-web-identity in environment to run Wed identity tests"
        end
      end

      context 'with AWS_ROLE_SESSION_NAME' do
        before do
          stub_const('ENV', ENV.to_hash.merge('AWS_ROLE_SESSION_NAME' => 'mongo-ruby-driver-test-app'))
        end

        it_behaves_like 'retrieves the credentials'
      end

      context 'without AWS_ROLE_SESSION_NAME' do
        before do
          env = ENV.to_hash.dup
          env.delete('AWS_ROLE_SESSION_NAME')
          stub_const('ENV', env)
        end

        it_behaves_like 'retrieves the credentials'
      end
    end
  end
end
