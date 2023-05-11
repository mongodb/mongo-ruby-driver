# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::User do

  let(:options) do
    { database: 'testing', user: 'user', password: 'pass' }
  end

  let(:user) do
    described_class.new(options)
  end

  shared_examples_for 'sets database and auth source to admin' do

    it 'sets database to admin' do
      expect(user.database).to eq('admin')
    end

    it 'sets auth source to admin' do
      expect(user.auth_source).to eq('admin')
    end
  end

  shared_examples_for 'sets auth source to $external' do

    it 'sets auth source to $external' do
      expect(user.auth_source).to eq('$external')
    end
  end

  describe '#initialize' do
    let(:user) { Mongo::Auth::User.new(options) }

    context 'no options' do
      let(:options) { {} }

      it 'succeeds' do
        expect(user).to be_a(Mongo::Auth::User)
      end

      it_behaves_like 'sets database and auth source to admin'
    end

    context 'invalid mechanism' do
      let(:options) { {auth_mech: :invalid} }

      it 'raises ArgumentError' do
        expect do
          user
        end.to raise_error(Mongo::Auth::InvalidMechanism, ":invalid is invalid, please use one of the following mechanisms: :aws, :gssapi, :mongodb_cr, :mongodb_x509, :plain, :scram, :scram256")
      end
    end

    context 'mechanism given as string' do
      let(:options) { {auth_mech: 'scram'} }

      context 'not linting' do
        require_no_linting

        it 'warns' do
          expect(Mongo::Logger.logger).to receive(:warn)
          user
        end

        it 'converts mechanism to symbol' do
          expect(user.mechanism).to eq(:scram)
        end

        it_behaves_like 'sets database and auth source to admin'
      end

      context 'linting' do
        require_linting

        it 'raises LintError' do
          expect do
            user
          end.to raise_error(Mongo::Error::LintError, "Auth mechanism \"scram\" must be specified as a symbol")
        end
      end
    end

    context 'mechanism given as symbol' do
      let(:options) { {auth_mech: :scram} }

      it 'does not warn' do
        expect(Mongo::Logger.logger).not_to receive(:warn)
        user
      end

      it 'stores mechanism' do
        expect(user.mechanism).to eq(:scram)
      end

      it_behaves_like 'sets database and auth source to admin'
    end

    context 'mechanism is x509' do
      let(:options) { {auth_mech: :mongodb_x509} }

      it 'sets database to admin' do
        expect(user.database).to eq('admin')
      end

      it_behaves_like 'sets auth source to $external'

      context 'database is explicitly given' do
        let(:options) { {auth_mech: :mongodb_x509, database: 'foo'} }

        it 'sets database to the specified one' do
          expect(user.database).to eq('foo')
        end

        it_behaves_like 'sets auth source to $external'
      end
    end

    it 'sets the database' do
      expect(user.database).to eq('testing')
    end

    it 'sets the name' do
      expect(user.name).to eq('user')
    end

    it 'sets the password' do
      expect(user.password).to eq('pass')
    end
  end

  describe '#auth_key' do

    let(:nonce) do

    end

    let(:expected) do
      Digest::MD5.hexdigest("#{nonce}#{user.name}#{user.hashed_password}")
    end

    it 'returns the users authentication key' do
      expect(user.auth_key(nonce)).to eq(expected)
    end
  end

  describe '#encoded_name' do

    context 'when the user name contains an =' do

      let(:options) do
        { user: 'user=' }
      end

      it 'escapes the = character to =3D' do
        expect(user.encoded_name).to eq('user=3D')
      end

      it 'returns a UTF-8 string' do
        expect(user.encoded_name.encoding.name).to eq('UTF-8')
      end
    end

    context 'when the user name contains a ,' do

      let(:options) do
        { user: 'user,' }
      end

      it 'escapes the , character to =2C' do
        expect(user.encoded_name).to eq('user=2C')
      end

      it 'returns a UTF-8 string' do
        expect(user.encoded_name.encoding.name).to eq('UTF-8')
      end
    end

    context 'when the user name contains no special characters' do

      it 'does not alter the user name' do
        expect(user.name).to eq('user')
      end

      it 'returns a UTF-8 string' do
        expect(user.encoded_name.encoding.name).to eq('UTF-8')
      end
    end
  end

  describe '#hashed_password' do

    let(:expected) do
      Digest::MD5.hexdigest("user:mongo:pass")
    end

    it 'returns the hashed password' do
      expect(user.hashed_password).to eq(expected)
    end

    context 'password not given' do
      let(:options) { {user: 'foo'} }

      it 'raises MissingPassword' do
        expect do
          user.hashed_password
        end.to raise_error(Mongo::Error::MissingPassword)
      end
    end
  end

  describe '#sasl_prepped_password' do

    let(:expected) do
      'pass'
    end

    it 'returns the clear text password' do
      expect(user.send(:sasl_prepped_password)).to eq(expected)
    end

    it 'returns the password encoded in utf-8' do
      expect(user.sasl_prepped_password.encoding.name).to eq('UTF-8')
    end

    context 'password not given' do
      let(:options) { {user: 'foo'} }

      it 'raises MissingPassword' do
        expect do
          user.sasl_prepped_password
        end.to raise_error(Mongo::Error::MissingPassword)
      end
    end
  end

  describe '#mechanism' do

    context 'when the option is provided' do

      let(:options) do
        { database: 'testing', user: 'user', password: 'pass', auth_mech: :plain }
      end

      let(:user) do
        described_class.new(options)
      end

      it 'returns the option' do
        expect(user.mechanism).to eq(:plain)
      end
    end

    context 'when no option is provided' do

      let(:user) do
        described_class.new(options)
      end

      it 'returns the default' do
        expect(user.mechanism).to be_nil
      end
    end
  end

  describe '#auth_mech_properties' do

    context 'when the option is provided' do

      let(:auth_mech_properties) do
        { service_name: 'test',
          service_realm: 'test',
          canonicalize_host_name: true }
      end

      let(:options) do
        { database: 'testing', user: 'user', password: 'pass', auth_mech_properties: auth_mech_properties }
      end

      let(:user) do
        described_class.new(options)
      end

      it 'returns the option' do
        expect(user.auth_mech_properties).to eq(auth_mech_properties)
      end
    end

    context 'when no option is provided' do

      let(:user) do
        described_class.new(options)
      end

      it 'returns an empty hash' do
        expect(user.auth_mech_properties).to eq({})
      end
    end
  end

  describe '#roles' do

    context 'when roles are provided' do

      let(:roles) do
        [ Mongo::Auth::Roles::ROOT ]
      end

      let(:user) do
        described_class.new(roles: roles)
      end

      it 'returns the roles' do
        expect(user.roles).to eq(roles)
      end
    end

    context 'when no roles are provided' do

      let(:user) do
        described_class.new({})
      end

      it 'returns an empty array' do
        expect(user.roles).to be_empty
      end
    end
  end

  describe '#spec' do
    context 'when no password and no roles are set' do
      let(:user) do
        described_class.new(user: 'foo')
      end

      it 'is a hash with empty roles' do
        user.spec.should == {roles: []}
      end
    end
  end
end
