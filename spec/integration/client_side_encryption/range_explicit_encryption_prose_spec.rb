# frozen_string_literal: true

require 'spec_helper'

describe 'Range Explicit Encryption' do
  min_server_version '6.2'
  require_libmongocrypt
  include_context 'define shared FLE helpers'

  let(:key1_id) do
    key1_document['_id']
  end

  before(:each) do
    authorized_client['explicit_encryption'].drop(encrypted_fields: encrypted_fields)
    authorized_client['explicit_encryption'].create(encrypted_fields: encrypted_fields)
    authorized_client.use(key_vault_db)[key_vault_coll].drop
    authorized_client.use(key_vault_db)[key_vault_coll, write_concern: {w: :majority}].insert_one(key1_document)
  end

  let(:key_vault_client) do
    ClientRegistry.instance.new_local_client(SpecConfig.instance.addresses)
  end

  let(:client_encryption) do
    Mongo::ClientEncryption.new(
      key_vault_client,
      kms_tls_options: kms_tls_options,
      key_vault_namespace: key_vault_namespace,
      kms_providers: local_kms_providers
    )
  end

  let(:encrypted_client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      auto_encryption_options: {
        key_vault_namespace: key_vault_namespace,
        kms_providers: local_kms_providers,
        bypass_query_analysis: true
      },
      database: SpecConfig.instance.test_db
    )
  end

  shared_examples 'common cases' do
    it 'can decrypt a payload' do
      value = value_converter.call(6)
      insert_payload = client_encryption.encrypt(
        value,
        {
          key_id: key1_id,
          algorithm: "RangePreview",
          contention_factor: 0,
          range_opts: range_opts
        }
      )
      decrypted_value = client_encryption.decrypt(insert_payload)
      expect(value).to eq(decrypted_value)
    end

    it 'can find encrypted range and return the maximum' do
      expr = {
        "$and": [
          {"encrypted#{type}" => {"$gte": value_converter.call(6)}},
          {"encrypted#{type}" => {"$lte": value_converter.call(200)}}
        ]
      }
      find_payload = client_encryption.encrypt_expression(
        expr,
        {
          key_id: key1_id,
          algorithm: "RangePreview",
          query_type: "rangePreview",
          contention_factor: 0,
          range_opts: range_opts
        }
      )
      results = encrypted_client['explicit_encryption'].find(find_payload, sort: {_id: 1}).to_a
      expect(results.size).to eq(3)
      value_converter.call([6, 30, 200]).each_with_index do |value, idx|
        expect(results[idx]["encrypted#{type}"]).to eq(value)
      end
    end

    it 'can find encrypted range and return the minimum' do
      expr = {
        "$and": [
          {"encrypted#{type}" => {"$gte": value_converter.call(0)}},
          {"encrypted#{type}" => {"$lte": value_converter.call(6)}}
        ]
      }
      find_payload = client_encryption.encrypt_expression(
        expr,
        {
          key_id: key1_id,
          algorithm: "RangePreview",
          query_type: "rangePreview",
          contention_factor: 0,
          range_opts: range_opts
        }
      )
      results = encrypted_client['explicit_encryption'].find(find_payload, sort: {_id: 1}).to_a
      expect(results.size).to eq(2)
      value_converter.call([0, 6]).each_with_index do |value, idx|
        expect(results[idx]["encrypted#{type}"]).to eq(value)
      end
    end

    it 'can find encrypted range with an open range query' do
      expr = {
        "$and": [
          { "encrypted#{type}" => { "$gt": value_converter.call(30) } }
        ]
      }
      find_payload = client_encryption.encrypt_expression(
        expr,
        {
          key_id: key1_id,
          algorithm: "RangePreview",
          query_type: "rangePreview",
          contention_factor: 0,
          range_opts: range_opts
        }
      )
      results = encrypted_client['explicit_encryption'].find(find_payload, sort: {_id: 1}).to_a
      expect(results.size).to eq(1)
      expect(results.first["encrypted#{type}"]).to eq(value_converter.call(200))
    end

    it 'can run an aggregation expression inside $expr' do
      expr = {'$and': [ { '$lt': [ "$encrypted#{type}", value_converter.call(30) ] } ] }
      find_payload = client_encryption.encrypt_expression(
        expr,
        {
          key_id: key1_id,
          algorithm: "RangePreview",
          query_type: "rangePreview",
          contention_factor: 0,
          range_opts: range_opts
        }
      )
      results = encrypted_client['explicit_encryption'].find(
        {'$expr' => find_payload},
        sort: { _id: 1 }
      ).to_a
      expect(results.size).to eq(2)
      value_converter.call([0, 6]).each_with_index do |value, idx|
        expect(results[idx]["encrypted#{type}"]).to eq(value)
      end
    end

    it 'encrypting a document greater than the maximum errors' do
      skip if type == 'DoubleNoPrecision'
      expect do
        client_encryption.encrypt(
          value_converter.call(201),
          {
            key_id: key1_id,
            algorithm: "RangePreview",
            contention_factor: 0,
            range_opts: range_opts
          }
        )
      end.to raise_error(Mongo::Error::CryptError, /less than or equal to the maximum value/)
    end

    it 'encrypting a document of a different type errors ' do
      skip if type == 'DoubleNoPrecision'
      expect do
        client_encryption.encrypt(
          6.0,
          {
            key_id: key1_id,
            algorithm: "RangePreview",
            contention_factor: 0,
            range_opts: range_opts
          }
        )
      end.to raise_error(Mongo::Error::CryptError, /expected matching 'min' and value type/)
    end

    it 'setting precision errors if the type is not a double' do
      skip if type != 'DoubleNoPrecision'
    end
  end


  context 'Int' do
    let(:type) do
      'Int'
    end

    let(:value_converter) do
      Proc.new do |value|
        if value.is_a?(Array)
          value.map(&:to_i)
        else
          value.to_i
        end
      end
    end

    let(:encrypted_fields) do
      range_encrypted_fields_int
    end

    let(:range_opts) do
      {
        min: BSON::Int32.new(0),
        max: BSON::Int32.new(200),
        sparsity: 1
      }
    end

    before(:each) do
      [0, 6, 30, 200].each_with_index do |num, idx|
        insert_payload = client_encryption.encrypt(
          num,
          key_id: key1_id,
          algorithm: "RangePreview",
          contention_factor: 0,
          range_opts: range_opts
        )
        encrypted_client['explicit_encryption'].insert_one(
          _id: idx,
          "encrypted#{type}" => insert_payload
        )
      end
    end

    include_examples 'common cases'
  end

  context 'Long' do
    let(:type) do
      'Long'
    end

    let(:value_converter) do
      Proc.new do |value|
        if value.is_a?(Array)
          value.map { |i| BSON::Int64.new(i)}
        else
          BSON::Int64.new(value)
        end
      end
    end

    let(:encrypted_fields) do
      range_encrypted_fields_long
    end

    let(:range_opts) do
      {
        min: BSON::Int64.new(0),
        max: BSON::Int64.new(200),
        sparsity: 1
      }
    end

    before(:each) do
      [0, 6, 30, 200].each_with_index do |num, idx|
        insert_payload = client_encryption.encrypt(
          BSON::Int64.new(num),
          key_id: key1_id,
          algorithm: "RangePreview",
          contention_factor: 0,
          range_opts: range_opts
        )
        encrypted_client['explicit_encryption'].insert_one(
          _id: idx,
          "encrypted#{type}" => insert_payload
        )
      end
    end

    include_examples 'common cases'
  end
end
