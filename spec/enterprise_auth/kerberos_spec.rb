require 'mongo'

describe 'kerberos authentication' do
  let(:user) do
   "#{ENV['SASL_USER']}%40#{ENV['SASL_HOST'].upcase}"
  end

  let(:host) do
    "#{ENV['SASL_HOST']}"
  end

  let(:kerberos_db) do
    "#{ENV['KERBEROS_DB']}"
  end

  let(:auth_source) do
    "#{ENV['SASL_DB']}"
  end

  let(:uri) do
    uri = "mongodb://#{user}@#{host}/#{kerberos_db}?authMechanism=GSSAPI&authSource=#{auth_source}"
  end

  let(:client) do
    Mongo::Client.new(uri)
  end

  before do
    skip unless ENV['ENTERPRISE_AUTH_TESTS']
  end

  let(:doc) do
    require 'mongo_kerberos'

    client.database[:test].find.first
  end

  it 'correctly authenticates' do
    expect(doc['kerberos']).to eq(true)
    expect(doc['authenticated']).to eq('yeah')
  end
end
