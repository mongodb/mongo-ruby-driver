# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

# max_pool_size is set to 1 to force a single connection being used for
# all operations in a client.

describe 'SCRAM-SHA auth mechanism negotiation' do
  min_server_fcv '4.0'
  require_no_external_user
  require_topology :single, :replica_set, :sharded
  # Test uses global assertions
  clean_slate

  let(:create_user!) do
    root_authorized_admin_client.tap do |client|
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
        SpecConfig.instance.test_options.merge(opts).update(max_pool_size: 1)
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

          before do
            create_user!
          end

          it 'authenticates successfully' do
            RSpec::Mocks.with_temporary_scope do
              mechanism = nil
              # With speculative auth, Auth is instantiated twice.
              expect(Mongo::Auth).to receive(:get).at_least(:once).at_most(:twice).and_wrap_original do |m, user, connection|
                # copy mechanism here rather than whole user
                # in case something mutates mechanism later
                mechanism = user.mechanism
                m.call(user, connection)
              end

              expect do
                result
              end.not_to raise_error
              expect(mechanism).to eq(:scram)
            end
          end
        end

        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          before do
            create_user!
          end

          it 'authenticates successfully with SCRAM-SHA-256' do
            RSpec::Mocks.with_temporary_scope do
              mechanism = nil
              # With speculative auth, Auth is instantiated twice.
              expect(Mongo::Auth).to receive(:get).at_least(:once).at_most(:twice).and_wrap_original do |m, user, connection|
                # copy mechanism here rather than whole user
                # in case something mutates mechanism later
                mechanism = user.mechanism
                m.call(user, connection)
              end

              expect { result }.not_to raise_error
              expect(mechanism).to eq(:scram256)
            end
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
      Utils.create_mongodb_uri(
        SpecConfig.instance.addresses,
        username: user.name,
        password: password,
        uri_options: SpecConfig.instance.uri_options.merge(
          auth_mech: auth_mech,
        ),
      )
    end

    let(:client) do
      new_local_client(uri, SpecConfig.instance.monitoring_options.merge(max_pool_size: 1))
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
            auth_mech: auth_mech,
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
            auth_mech: auth_mech,
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
            auth_mech: auth_mech,
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

          before do
            create_user!
            expect(user.mechanism).to eq(:scram)
          end

          it 'authenticates successfully' do
            RSpec::Mocks.with_temporary_scope do
              mechanism = nil
              # With speculative auth, Auth is instantiated twice.
              expect(Mongo::Auth).to receive(:get).at_least(:once).at_most(:twice).and_wrap_original do |m, user, connection|
                # copy mechanism here rather than whole user
                # in case something mutates mechanism later
                mechanism = user.mechanism
                m.call(user, connection)
              end

              expect { result }.not_to raise_error
              expect(mechanism).to eq(:scram)
            end
          end
        end

        context 'when SCRAM-SHA-256 is specified as the auth mechanism' do

          let(:auth_mech) do
            :scram256
          end

          before do
            create_user!
          end

          it 'authenticates successfully with SCRAM-SHA-256' do
            RSpec::Mocks.with_temporary_scope do
              mechanism = nil
              # With speculative auth, Auth is instantiated twice.
              expect(Mongo::Auth).to receive(:get).at_least(:once).at_most(:twice).and_wrap_original do |m, user, connection|
                # copy mechanism here rather than whole user
                # in case something mutates mechanism later
                mechanism = user.mechanism
                m.call(user, connection)
              end

              expect { result }.not_to raise_error
              expect(mechanism).to eq(:scram256)
            end
          end
        end
      end
    end
  end
end
