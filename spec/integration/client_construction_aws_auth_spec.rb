require 'spec_helper'

describe 'Client construction with AWS auth' do
  require_aws_auth

  let(:client) do
    new_local_client(SpecConfig.instance.addresses,
      SpecConfig.instance.ssl_options.merge(
        auth_mech: :aws,
        connect_timeout: 3.44, socket_timeout: 3.45,
        server_selection_timeout: 3.46))
  end

  let(:authenticated_user_info) do
    # https://stackoverflow.com/questions/21414608/mongodb-show-current-user
    info = client.database.command(connectionStatus: 1).documents.first
    info[:authInfo][:authenticatedUsers].first
  end

  let(:authenticated_user_name) { authenticated_user_info[:user] }

  shared_examples_for 'connects successfully' do
    it 'connects successfully' do
      client['foo'].insert_one(test: true)
    end
  end

  context 'credentials specified via environment' do
    require_auth 'aws-regular', 'aws-assume-role'

    context 'no credentials given explicitly to Client constructor' do
      context 'credentials not provided in environment' do
        local_env(
          'AWS_ACCESS_KEY_ID' => nil,
          'AWS_SECRET_ACCESS_KEY' => nil,
          'AWS_SESSION_TOKEN' => nil,
        )

        it 'does not connect' do
          lambda do
            client['foo'].insert_one(test: true)
          end.should raise_error(Mongo::Auth::InvalidConfiguration, /Could not locate AWS credentials/)
        end
      end

      context 'credentials provided in environment' do
        local_env do
          {
            'AWS_ACCESS_KEY_ID' => ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
            'AWS_SECRET_ACCESS_KEY' => ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
            'AWS_SESSION_TOKEN' => ENV['MONGO_RUBY_DRIVER_AWS_AUTH_SESSION_TOKEN'],
          }
        end

        it_behaves_like 'connects successfully'

        context 'when using regular credentials' do
          require_auth 'aws-regular'

          it 'uses the expected user' do
            puts "Authenticated as #{authenticated_user_name}"
            authenticated_user_name.should =~ /^arn:/
            authenticated_user_name.should_not =~ /^arn:.*assumed-role/
          end
        end

        context 'when using assume role credentials' do
          require_auth 'aws-assume-role'

          it 'uses the expected user' do
            puts "Authenticated as #{authenticated_user_name}"
            authenticated_user_name.should =~ /^arn:.*assumed-role/
          end
        end
      end
    end
  end

  context 'credentials specified via instance/task metadata' do
    require_auth 'aws-ec2', 'aws-ecs'

    before(:all) do
      # No explicit credentials are expected in the tested configurations
      ENV['AWS_ACCESS_KEY_ID'].should be_nil
    end

    it_behaves_like 'connects successfully'

    context 'when using ec2 instance role' do
      require_auth 'aws-ec2'

      it 'uses the expected user' do
        puts "Authenticated as #{authenticated_user_name}"
        authenticated_user_name.should =~ /^arn:aws:sts:.*assumed-role.*instance_profile_role/
      end
    end

    context 'when using ecs task role' do
      require_auth 'aws-ecs'

      it 'uses the expected user' do
        puts "Authenticated as #{authenticated_user_name}"
        authenticated_user_name.should =~ /^arn:aws:sts:.*assumed-role.*ecstaskexecutionrole/i
      end
    end
  end

end
