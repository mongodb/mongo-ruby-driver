require 'spec_helper'

describe 'Auth' do
  include Mongo::Auth
  
  MECHANISMS = {
    'MONGODB-CR' => :mongodb_cr,
    'MONGODB-X509' => :mongodb_x509,
    'PLAIN' => :plain,
    'SCRAM-SHA-1' => :scram,
    'SCRAM-SHA-256' => :scram256,
    'GSSAPI' => :gssapi
  }

  clean_slate_for_all

  AUTH_TESTS.each do |file|
    spec = Mongo::Auth::Spec.new(file)

    context(spec.description) do
      spec.tests.each_with_index do |test, index|
        context test.description do
          if test.description.downcase.include?("gssapi")
            require_mongo_kerberos
          end

          context 'when the auth configuration is invalid', unless: test.valid? do
            it 'raises an error' do
              expect {
                test.client
            }.to raise_error(Mongo::Auth::InvalidConfiguration)
            end
          end

          context 'when the auth configuration is valid', if: test.valid? do
            let(:client) { test.client }
            let(:credential) { test.credential }

            context 'when credential is empty', if: test.valid? && test.credential.nil? do
              it 'does not create a credential' do
                %i(user password auth_source auth_mech auth_mech_properties).each do |opt|
                  expect(client.options[opt]).to be_nil
                end
              end
            end

            context 'when credential is not empty', unless: test.valid? && test.credential.nil? do
              it 'creates a client with the correct user' do
                expect(client.options[:user]).to eq(credential['username'])
              end

              it 'creates a client with the correct password' do
                expect(client.options[:password]).to eq(credential['password'])
              end

              it 'creates a client with the correct auth source' do
                expected_auth_source = credential['source'] == '$external' ? :external : credential['source']
                expect(client.options[:auth_source]).to eq(expected_auth_source)
              end

              it 'creates a client with the correct auth mechanism' do
                if credential['mechanism'].nil?
                  expected_mechanism = nil
                else
                  expected_mechanism = MECHANISMS[credential['mechanism']]
                end

                expect(client.options[:auth_mech]).to eq(expected_mechanism)
              end

              it 'creates a client with the correct auth mechanism properties' do
                if credential['mechanism_properties'].nil?
                  expect(client.options[:auth_mech_properties]).to be_nil
                else
                  expect(client.options[:auth_mech_properties].keys).to eq(credential['mechanism_properties'].keys.map(&:downcase))

                  credential['mechanism_properties'].each do |prop, prop_val|
                    expect(client.options[:auth_mech_properties][prop.downcase]).to eq(prop_val)
                  end
                end
              end
            end
          end
        end
      end
    end
  end
end
