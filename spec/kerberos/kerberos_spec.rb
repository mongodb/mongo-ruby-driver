# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe 'kerberos authentication' do
  require_mongo_kerberos

  before(:all) do
    unless %w(1 yes true).include?(ENV['MONGO_RUBY_DRIVER_KERBEROS_INTEGRATION']&.downcase)
      skip "Set MONGO_RUBY_DRIVER_KERBEROS_INTEGRATION=1 in environment to run the Kerberos integration tests"
    end
  end

  def require_env_value(key)
    ENV[key].tap do |value|
      if value.nil? || value.empty?
        raise "Value for key #{key} is not present in environment as required"
      end
    end
  end

  let(:user) do
   "#{require_env_value('SASL_USER')}%40#{realm}"
  end

  let(:host) do
    require_env_value('SASL_HOST')
  end

  let(:realm) do
    require_env_value('SASL_REALM')
  end

  let(:kerberos_db) do
    require_env_value('KERBEROS_DB')
  end

  let(:auth_source) do
    require_env_value('SASL_DB')
  end

  let(:uri) do
    uri = "mongodb://#{user}@#{host}/#{kerberos_db}?authMechanism=GSSAPI&authSource=#{auth_source}"
  end

  let(:client) do
    Mongo::Client.new(uri, server_selection_timeout: 6.31)
  end

  let(:doc) do
    client.database[:test].find.first
  end

  shared_examples_for 'correctly authenticates' do
    it 'correctly authenticates' do
      expect(doc['kerberos']).to eq(true)
      expect(doc['authenticated']).to eq('yeah')
    end
  end

  it_behaves_like 'correctly authenticates'

  context 'when host is lowercased' do
    let(:host) do
      require_env_value('SASL_HOST').downcase
    end

    it_behaves_like 'correctly authenticates'
  end

  context 'when host is uppercased' do
    let(:host) do
      require_env_value('SASL_HOST').upcase
    end

    it_behaves_like 'correctly authenticates'
  end

  context 'when canonicalize_host_name is true' do
    let(:host) do
      "#{require_env_value('IP_ADDR')}"
    end

    let(:uri) do
      uri = "mongodb://#{user}@#{host}/#{kerberos_db}?authMechanism=GSSAPI&authSource=#{auth_source}&authMechanismProperties=CANONICALIZE_HOST_NAME:true"
    end

    it 'correctly authenticates when using the IP' do
      expect(doc['kerberos']).to eq(true)
      expect(doc['authenticated']).to eq('yeah')
    end
  end
end
