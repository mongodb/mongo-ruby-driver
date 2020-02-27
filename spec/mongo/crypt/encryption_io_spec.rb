require 'mongo'
require 'spec_helper'

describe Mongo::Crypt::EncryptionIO do
  let(:subject) do
    described_class.new(
      key_vault_namespace: 'foo.bar',
      key_vault_client: authorized_client,
      mongocryptd_options: mongocryptd_options,
    )
  end

  describe '#spawn_mongocryptd' do
    context 'no spawn args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
        }
      end

      it 'fails with an exception' do
        lambda do
          subject.send(:spawn_mongocryptd)
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process without providing.*mongocryptd_spawn_args/)
      end
    end

    context 'empty array for spawn args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
          mongocryptd_spawn_args: [],
        }
      end

      it 'fails with an exception' do
        lambda do
          subject.send(:spawn_mongocryptd)
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process without providing.*mongocryptd_spawn_args/)
      end
    end

    context 'good spawn path and args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
          mongocryptd_spawn_args: ['hi'],
        }
      end

      it 'spawns' do
        subject.send(:spawn_mongocryptd)
      end
    end
  end
end
