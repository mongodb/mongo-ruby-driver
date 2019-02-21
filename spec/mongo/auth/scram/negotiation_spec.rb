require 'spec_helper'

describe 'SCRAM-SHA auth mechanism negotiation' do
  min_server_fcv '4.0'

  before(:all) do
    ClientRegistry.instance.close_all_clients
  end

  URI_OPTION_MAP = {
    :auth_source => 'authsource',
    :replica_set => 'replicaSet',
  }

  let(:create_user!) do
    ClientRegistry.instance.global_client('root_authorized_admin').tap do |client|
      users = client.database.users
      if users.info(user.name).any?
        users.remove(user.name)
      end
      client.database.command(
        createUser: user.name,
        pwd: password,
        roles: ['root'],
        mechanisms: server_user_auth_mechanisms,
      )
      client.close
    end
  end

  let(:password) do
    user.password
  end

  let(:result) do
    client.database['admin'].find(nil, limit: 1).first
  end

  context 'when the configuration is specified in code' do

    let(:client) do
      opts = {
        database: 'admin',
        user: user.name,
        password: password
      }.tap do |o|
        o[:auth_mech] = auth_mech if auth_mech
      end

      new_local_client(
        SpecConfig.instance.addresses,
        SpecConfig.instance.test_options.merge(opts)
      )
    end

    context 'when the user exists' do

      context 'when the user only can use SCRAM-SHA-1 to authenticate' do

        let(:server_user_auth_mechanisms) do
          ['SCRAM-SHA-1']
        end

        let(:user) do
          Mongo::Auth::User.new(
            user: 'sha1',
            password: 'sha1',
            auth_mech: auth_mech
          )
        end

        context 'when no auth mechanism is specified' do

          let(:auth_mech) do
            nil
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end

        context 'when SCRAM-SHA-1 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end


        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          it 'fails with a Mongo::Auth::Unauthorized error' do
            create_user!
            expect { result }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end
      end

      context 'when the user only can use SCRAM-SHA-256 to authenticate' do

        let(:server_user_auth_mechanisms) do
          ['SCRAM-SHA-256']
        end

        let(:user) do
          Mongo::Auth::User.new(
            user: 'sha256',
            password: 'sha256',
            auth_mech: auth_mech
          )
        end

        context 'when no auth mechanism is specified' do

          let(:auth_mech) do
            nil
          end

          it 'authenticates successfully' do
            create_user!

            expect { client.database['admin'].find(options = { limit: 1 }).first }.not_to raise_error
          end
        end

        context 'when SCRAM-SHA-1 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram
          end

          it 'fails with a Mongo::Auth::Unauthorized error' do
            create_user!

            expect { result }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end

        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end
      end

      context 'when the user only can use either SCRAM-SHA-1 or SCRAM-SHA-256 to authenticate' do

        let(:server_user_auth_mechanisms) do
          ['SCRAM-SHA-1', 'SCRAM-SHA-256']
        end

        let(:user) do
          Mongo::Auth::User.new(
            user: 'both',
            password: 'both',
            auth_mech: auth_mech
          )
        end

        context 'when no auth mechanism is specified' do

          let(:auth_mech) do
            nil
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end

        context 'when SCRAM-SHA-1 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram
          end

          it 'authenticates successfully' do
            create_user!

            mechanism = nil
            expect(Mongo::Auth).to receive(:get).and_wrap_original do |m, user|
              # copy mechanism here rather than whole user
              # in case something mutates mechanism later
              mechanism = user.mechanism
              m.call(user)
            end

            expect { result }.not_to raise_error
            expect(mechanism).to eq(:scram)
          end
        end

        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          it 'authenticates successfully with SCRAM-SHA-256' do
            create_user!

            mechanism = nil
            expect(Mongo::Auth).to receive(:get).and_wrap_original do |m, user|
              # copy mechanism here rather than whole user
              # in case something mutates mechanism later
              mechanism = user.mechanism
              m.call(user)
            end

            expect { result }.not_to raise_error
            expect(mechanism).to eq(:scram256)
          end
        end
      end
    end

    context 'when the user does not exist' do

      let(:auth_mech) do
        nil
      end

      let(:user) do
        Mongo::Auth::User.new(
          user: 'nonexistent',
          password: 'nonexistent',
        )
      end

      it 'fails with a Mongo::Auth::Unauthorized error' do
        expect { result }.to raise_error(Mongo::Auth::Unauthorized)
      end
    end

    context 'when the username and password provided require saslprep' do

      let(:auth_mech) do
        nil
      end

      let(:server_user_auth_mechanisms) do
        ['SCRAM-SHA-256']
      end

      context 'when the username and password as ASCII' do

        let(:user) do
          Mongo::Auth::User.new(
            user: 'IX',
            password: 'IX'
          )
        end

        let(:password) do
          "I\u00ADX"
        end

        it 'authenticates successfully after saslprepping password' do
          create_user!

          expect { result }.not_to raise_error
        end
      end

      context 'when the username and password are non-ASCII' do

        let(:user) do
          Mongo::Auth::User.new(
            user: "\u2168",
            password: "\u2163"
          )
        end

        let(:password) do
          "I\u00ADV"
        end

        it 'authenticates successfully after saslprepping password' do
          create_user!

          expect { result }.not_to raise_error
        end
      end
    end
  end

  context 'when the configuration is specified in the URI' do

    let(:uri) do
      "mongodb://#{user.name}:#{password}@#{SpecConfig.instance.addresses.join(',')}/admin".tap do |uri|
        first = true

        if SpecConfig.instance.uri_options
          SpecConfig.instance.uri_options.each do |k, v|
            uri << (first ? '?' : '&')
            first = false

            k = URI_OPTION_MAP[k] || k

            uri << "#{k}=#{v}"
          end
        end

        if auth_mech
          uri << (first ? '?' : '&')

          uri << "authMechanism=#{Mongo::URI::AUTH_MECH_MAP.key(auth_mech)}"
        end
      end
    end

    let(:client) do
      new_local_client(uri, SpecConfig.instance.ssl_options)
    end

    context 'when the user exists' do

      context 'when the user only can use SCRAM-SHA-1 to authenticate' do

        let(:server_user_auth_mechanisms) do
          ['SCRAM-SHA-1']
        end

        let(:user) do
          Mongo::Auth::User.new(
            user: 'sha1',
            password: 'sha1',
            auth_mech: auth_mech
          )
        end

        context 'when no auth mechanism is specified' do

          let(:auth_mech) do
            nil
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end

        context 'when SCRAM-SHA-1 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end

        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          it 'fails with a Mongo::Auth::Unauthorized error' do
            create_user!
            expect { result }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end
      end

      context 'when the user only can use SCRAM-SHA-256 to authenticate' do

        let(:server_user_auth_mechanisms) do
          ['SCRAM-SHA-256']
        end

        let(:user) do
          Mongo::Auth::User.new(
            user: 'sha256',
            password: 'sha256',
            auth_mech: auth_mech
          )
        end

        context 'when no auth mechanism is specified' do

          let(:auth_mech) do
            nil
          end

          it 'authenticates successfully' do
            create_user!

            expect { client.database['admin'].find(options = { limit: 1 }).first }.not_to raise_error
          end
        end

        context 'when SCRAM-SHA-1 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram
          end

          it 'fails with a Mongo::Auth::Unauthorized error' do
            create_user!

            expect { result }.to raise_error(Mongo::Auth::Unauthorized)
          end
        end

        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end
      end

      context 'when the user only can use either SCRAM-SHA-1 or SCRAM-SHA-256 to authenticate' do

        let(:server_user_auth_mechanisms) do
          ['SCRAM-SHA-1', 'SCRAM-SHA-256']
        end

        let(:user) do
          Mongo::Auth::User.new(
            user: 'both',
            password: 'both',
            auth_mech: auth_mech
          )
        end

        context 'when no auth mechanism is specified' do

          let(:auth_mech) do
            nil
          end

          it 'authenticates successfully' do
            create_user!

            expect { result }.not_to raise_error
          end
        end

        context 'when SCRAM-SHA-1 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram
          end

          it 'authenticates successfully' do
            create_user!
            expect(user.mechanism).to eq(:scram)

            mechanism = nil
            expect(Mongo::Auth).to receive(:get).and_wrap_original do |m, user|
              # copy mechanism here rather than whole user
              # in case something mutates mechanism later
              mechanism = user.mechanism
              m.call(user)
            end

            expect { result }.not_to raise_error
            expect(mechanism).to eq(:scram)
          end
        end

        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          it 'authenticates successfully with SCRAM-SHA-256' do
            create_user!

            mechanism = nil
            expect(Mongo::Auth).to receive(:get).and_wrap_original do |m, user|
              # copy mechanism here rather than whole user
              # in case something mutates mechanism later
              mechanism = user.mechanism
              m.call(user)
            end

            expect { result }.not_to raise_error
            expect(mechanism).to eq(:scram256)
          end
        end
      end
    end
  end
end
