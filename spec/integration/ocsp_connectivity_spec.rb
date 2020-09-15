require 'lite_spec_helper'

# These tests test the configurations described in
# https://github.com/mongodb/specifications/blob/master/source/ocsp-support/tests/README.rst#integration-tests-permutations-to-be-tested
describe 'OCSP connectivity' do
  require_ocsp_connectivity
  clear_ocsp_cache

  let(:client) do
    algorithm = ENV.fetch('OCSP_ALGORITHM')
    new_local_client(SpecConfig.instance.addresses,
      ssl: true,
      ssl_ca_cert: "spec/support/ocsp/#{algorithm}/ca.crt",
      ssl_cert: "spec/support/ocsp/#{algorithm}/server.pem",
      ssl_key: "spec/support/ocsp/#{algorithm}/server.pem",
      server_selection_timeout: 5,
    )
  end

  if ENV['OCSP_CONNECTIVITY'] == 'fail'
    it 'fails to connect' do
      lambda do
        client.command(ping: 1)
      end.should raise_error(Mongo::Error::NoServerAvailable, /UNKNOWN/)
    end
  else
    it 'works' do
      client.command(ping: 1)
    end
  end
end
