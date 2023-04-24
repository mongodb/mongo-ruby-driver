# frozen_string_literal: true
# rubocop:todo all

require 'mongo'
require 'spec_helper'

describe Mongo::Crypt::EncryptionIO do
  let(:subject) do
    described_class.new(
      key_vault_namespace: 'foo.bar',
      key_vault_client: authorized_client,
      metadata_client: authorized_client.with(auto_encryption_options: nil),
      mongocryptd_options: mongocryptd_options,
    )
  end

  describe '#spawn_mongocryptd' do
    context 'no spawn path' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_args: ['test'],
        }
      end

      it 'fails with an exception' do
        lambda do
          subject.send(:spawn_mongocryptd)
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process when no.*mongocryptd_spawn_path/)
      end
    end

    context 'no spawn args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
        }
      end

      it 'fails with an exception' do
        lambda do
          subject.send(:spawn_mongocryptd)
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process when no.*mongocryptd_spawn_args/)
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
        end.should raise_error(ArgumentError, /Cannot spawn mongocryptd process when no.*mongocryptd_spawn_args/)
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

    context '-- for args to emulate no args' do
      let(:mongocryptd_options) do
        {
          mongocryptd_spawn_path: 'echo',
          mongocryptd_spawn_args: ['--'],
        }
      end

      it 'spawns' do
        subject.send(:spawn_mongocryptd)
      end
    end
  end

  describe '#mark_command' do
    let(:mock_client) do
      double('mongocryptd client').tap do |client|
        database = double('mock database')
        expect(database).to receive(:command).and_raise(Mongo::Error::NoServerAvailable.new(Mongo::ServerSelector::Primary.new, nil, 'test message'))
        allow(database).to receive(:command).and_return([])
        expect(client).to receive(:database).at_least(:once).and_return(database)
      end
    end

    let(:base_options) do
      {
        mongocryptd_spawn_path: 'echo',
        mongocryptd_spawn_args: ['--'],
      }
    end

    let(:subject) do
      described_class.new(
        mongocryptd_client: mock_client,
        key_vault_namespace: 'foo.bar',
        key_vault_client: authorized_client,
        metadata_client: authorized_client.with(auto_encryption_options: nil),
        mongocryptd_options: mongocryptd_options,
      )
    end

    context ':mongocryptd_bypass_spawn not given' do
      let(:mongocryptd_options) do
        base_options
      end

      it 'spawns' do
        expect(subject).to receive(:spawn_mongocryptd)
        subject.mark_command({})
      end
    end

    context ':mongocryptd_bypass_spawn given' do
      let(:mongocryptd_options) do
        base_options.merge(
          mongocryptd_bypass_spawn: true,
        )
      end

      it 'does not spawn' do
        expect(subject).not_to receive(:spawn_mongocryptd)
        lambda do
          subject.mark_command({})
        end.should raise_error(Mongo::Error::NoServerAvailable, /test message/)
      end
    end
  end
end
