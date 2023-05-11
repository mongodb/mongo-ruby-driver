# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe Mongo::Auth::Aws::CredentialsCache do
  require_auth 'aws-ec2', 'aws-ecs', 'aws-web-identity'

  def new_client
    ClientRegistry.instance.new_authorized_client.tap do |client|
      @clients << client
    end
  end

  before do
    @clients = []
    described_class.instance.clear
  end

  after do
    @clients.each(&:close)
  end

  it 'caches the credentials' do
    client1 = new_client
    client1['test-collection'].find.to_a
    expect(described_class.instance.credentials).not_to be_nil

    described_class.instance.credentials = Mongo::Auth::Aws::Credentials.new(
      described_class.instance.credentials.access_key_id,
      described_class.instance.credentials.secret_access_key,
      described_class.instance.credentials.session_token,
      Time.now + 60
    )
    client2 = new_client
    client2['test-collection'].find.to_a
    expect(described_class.instance.credentials).not_to be_expired

    described_class.instance.credentials = Mongo::Auth::Aws::Credentials.new(
      'bad_access_key_id',
      described_class.instance.credentials.secret_access_key,
      described_class.instance.credentials.session_token,
      described_class.instance.credentials.expiration
    )
    client3 = new_client
    expect { client3['test-collection'].find.to_a }.to raise_error(Mongo::Auth::Unauthorized)
    expect(described_class.instance.credentials).to be_nil
    expect { client3['test-collection'].find.to_a }.not_to raise_error
    expect(described_class.instance.credentials).not_to be_nil
  end
end
