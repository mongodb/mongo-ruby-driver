# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'SSL connections with URI options' do
  # SpecConfig currently creates clients exclusively through non-URI options.
  # Because we don't currently have a way to create what the URI would look
  # like for a given client, it's simpler just to test the that TLS works when
  # configured from a URI on a standalone server without auth required, since
  # that allows us to build the URI more easily.
  require_no_auth
  require_topology :single
  require_tls

  let(:hosts) do
    SpecConfig.instance.addresses.join(',')
  end

  let(:uri) do
    "mongodb://#{hosts}/?tls=true&tlsInsecure=true&tlsCertificateKeyFile=#{SpecConfig.instance.client_pem_path}"
  end

  it 'successfully connects and runs an operation' do
    client = new_local_client(uri)
    expect { client[:foo].count_documents }.not_to raise_error
  end
end
