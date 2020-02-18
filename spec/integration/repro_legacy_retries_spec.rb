require 'spec_helper'

describe 'Legacy Retries Error' do
  require_libmongocrypt
  require_enterprise
  min_server_fcv '4.2'

  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:encryption_client) do
    new_local_client(
      SpecConfig.instance.addresses,
      SpecConfig.instance.test_options.merge(
        auto_encryption_options: {
          kms_providers: kms_providers,
          key_vault_namespace: key_vault_namespace,
          schema_map: { "auto_encryption.users" => schema_map },
        },
        database: 'auto_encryption'
      ),
    )
  end

  before(:each) do
    authorized_client.use(key_vault_db)[key_vault_coll].drop
    authorized_client.use(key_vault_db)[key_vault_coll].insert_one(data_key)

    encryption_client[:users].drop
    result = encryption_client[:users].insert_one(ssn: ssn, age: 23)
  end

  it 'raises error mentioning legacy retries' do
    encryption_client[:users].update_one({ ssn: ssn }, { ssn: '555-555-5555' })

    # Raises:
    # Mongo::Error::OperationFailure:
    #    BSON field 'update.documents' is an unknown field. (40415) (on localhost:27020, on localhost:27017, legacy retry, attempt 1) (on localhost:27020, on localhost:27017, legacy retry, attempt 1)
  end
end
