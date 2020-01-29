require 'spec_helper'

describe 'Client with auto encryption after reconnect' do
  let(:client) do
    new_local_client(
      'mongodb://localhost:27017/test',
      {
        auto_encryption_options: {
          kms_providers: { local: { key: key } },
          key_vault_namespace: 'admin.datakeys',
        }
      }
    )
  end

  context 'when reconnecting without closing' do

  end

  context 'when reconnecting after closing' do

  end

  context 'after killing monitor thread' do

  end

  
end
