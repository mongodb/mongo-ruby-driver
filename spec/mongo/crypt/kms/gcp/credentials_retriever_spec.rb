require 'spec_helper'

describe Mongo::Crypt::KMS::GCP::CredentialsRetriever do
  it 'returns the token' do
    expect do
      described_class.get_access_token
    end.not_to raise_error
  end
end
