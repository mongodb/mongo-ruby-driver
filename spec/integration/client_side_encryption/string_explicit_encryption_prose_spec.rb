# frozen_string_literal: true

require 'spec_helper'

# Prose test 27 "String Explicit Encryption" from the client-side-encryption
# specification. Only the GA parameter sets (server 9.0.0+) are exercised here;
# the "*Preview" query types are not implemented in the driver.
describe 'String Explicit Encryption' do
  min_server_version '9.0.0-rc0'

  require_libmongocrypt
  include_context 'define shared FLE helpers'
  include_context 'with local kms_providers'

  let(:key1_id) { key1_document['_id'] }

  let(:prefix_suffix_fields) do
    BSON::ExtJSON.parse(File.read('spec/support/crypt/encrypted_fields/encryptedFields-prefix-suffix.json'))
  end

  let(:prefix_suffix_ci_di_fields) do
    BSON::ExtJSON.parse(File.read('spec/support/crypt/encrypted_fields/encryptedFields-prefix-suffix-ci-di.json'))
  end

  let(:substring_fields) do
    BSON::ExtJSON.parse(File.read('spec/support/crypt/encrypted_fields/encryptedFields-substring.json'))
  end

  let(:substring_ci_di_fields) do
    BSON::ExtJSON.parse(File.read('spec/support/crypt/encrypted_fields/encryptedFields-substring-ci-di.json'))
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

  # Client with auto encryption but query analysis bypassed, used to insert
  # explicitly encrypted payloads and to run explicitly encrypted queries.
  let(:explicit_encrypted_client) do
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

  # Client with full auto encryption (query analysis enabled), used to insert
  # documents that must be auto-encrypted.
  let(:auto_encrypted_client) do
    ClientRegistry.instance.new_local_client(
      SpecConfig.instance.addresses,
      auto_encryption_options: {
        key_vault_namespace: key_vault_namespace,
        kms_providers: local_kms_providers
      },
      database: SpecConfig.instance.test_db
    )
  end

  def create_collection(name, fields)
    authorized_client[name].drop(encrypted_fields: fields)
    authorized_client[name, write_concern: { w: :majority }].create(encrypted_fields: fields)
  end

  before do
    create_collection('prefix-suffix', prefix_suffix_fields)
    create_collection('prefix-suffix-ci-di', prefix_suffix_ci_di_fields)
    create_collection('substring', substring_fields)
    create_collection('substring-ci-di', substring_ci_di_fields)

    authorized_client.use(key_vault_db)[key_vault_coll].drop
    authorized_client.use(key_vault_db)[key_vault_coll, write_concern: { w: :majority }]
                     .insert_one(key1_document)

    # Insert a document with prefix+suffix indexes.
    prefix_suffix_insert = client_encryption.encrypt(
      'foobarbaz',
      key_id: key1_id,
      algorithm: 'String',
      contention_factor: 0,
      string_opts: {
        case_sensitive: true,
        diacritic_sensitive: true,
        prefix: { str_max_query_length: 10, str_min_query_length: 2 },
        suffix: { str_max_query_length: 10, str_min_query_length: 2 }
      }
    )
    explicit_encrypted_client['prefix-suffix', write_concern: { w: :majority }]
      .insert_one(_id: 0, encryptedText: prefix_suffix_insert)

    # Insert a document with a substring index.
    substring_insert = client_encryption.encrypt(
      'foobarbaz',
      key_id: key1_id,
      algorithm: 'String',
      contention_factor: 0,
      string_opts: {
        case_sensitive: true,
        diacritic_sensitive: true,
        substring: { str_max_length: 10, str_max_query_length: 6, str_min_query_length: 2 }
      }
    )
    explicit_encrypted_client['substring', write_concern: { w: :majority }]
      .insert_one(_id: 0, encryptedText: substring_insert)
  end

  # Encrypts +value+ for a prefix/suffix query.
  def encrypt_prefix_suffix(value, query_type)
    client_encryption.encrypt(
      value,
      key_id: key1_id,
      algorithm: 'String',
      query_type: query_type,
      contention_factor: 0,
      string_opts: {
        case_sensitive: true,
        diacritic_sensitive: true,
        query_type.to_sym => { str_max_query_length: 10, str_min_query_length: 2 }
      }
    )
  end

  def encrypt_substring(value, query_type)
    client_encryption.encrypt(
      value,
      key_id: key1_id,
      algorithm: 'String',
      query_type: query_type,
      contention_factor: 0,
      string_opts: {
        case_sensitive: true,
        diacritic_sensitive: true,
        substring: { str_max_length: 10, str_max_query_length: 6, str_min_query_length: 2 }
      }
    )
  end

  # Encrypts for a case/diacritic-insensitive prefix/suffix query.
  def encrypt_prefix_suffix_ci_di(value, query_type)
    client_encryption.encrypt(
      value,
      key_id: key1_id,
      algorithm: 'String',
      query_type: query_type,
      contention_factor: 0,
      string_opts: {
        case_sensitive: false,
        diacritic_sensitive: false,
        query_type.to_sym => { str_max_query_length: 10, str_min_query_length: 2 }
      }
    )
  end

  def encrypt_substring_ci_di(value, query_type)
    client_encryption.encrypt(
      value,
      key_id: key1_id,
      algorithm: 'String',
      query_type: query_type,
      contention_factor: 0,
      string_opts: {
        case_sensitive: false,
        diacritic_sensitive: false,
        substring: { str_max_length: 10, str_max_query_length: 6, str_min_query_length: 2 }
      }
    )
  end

  it 'Case 1: can find a document by prefix' do
    encrypted = encrypt_prefix_suffix('foo', 'prefix')
    result = explicit_encrypted_client['prefix-suffix'].find(
      '$expr' => { '$encStrStartsWith' => { input: '$encryptedText', prefix: encrypted } }
    ).to_a
    expect(result.map { |doc| doc.slice('_id', 'encryptedText') })
      .to eq([ { '_id' => 0, 'encryptedText' => 'foobarbaz' } ])
  end

  it 'Case 2: can find a document by suffix' do
    encrypted = encrypt_prefix_suffix('baz', 'suffix')
    result = explicit_encrypted_client['prefix-suffix'].find(
      '$expr' => { '$encStrEndsWith' => { input: '$encryptedText', suffix: encrypted } }
    ).to_a
    expect(result.map { |doc| doc.slice('_id', 'encryptedText') })
      .to eq([ { '_id' => 0, 'encryptedText' => 'foobarbaz' } ])
  end

  it 'Case 3: assert no document found by prefix' do
    encrypted = encrypt_prefix_suffix('baz', 'prefix')
    result = explicit_encrypted_client['prefix-suffix'].find(
      '$expr' => { '$encStrStartsWith' => { input: '$encryptedText', prefix: encrypted } }
    ).to_a
    expect(result).to be_empty
  end

  it 'Case 4: assert no document found by suffix' do
    encrypted = encrypt_prefix_suffix('foo', 'suffix')
    result = explicit_encrypted_client['prefix-suffix'].find(
      '$expr' => { '$encStrEndsWith' => { input: '$encryptedText', suffix: encrypted } }
    ).to_a
    expect(result).to be_empty
  end

  it 'Case 5: can find a document by substring' do
    encrypted = encrypt_substring('bar', 'substring')
    result = explicit_encrypted_client['substring'].find(
      '$expr' => { '$encStrContains' => { input: '$encryptedText', substring: encrypted } }
    ).to_a
    expect(result.map { |doc| doc.slice('_id', 'encryptedText') })
      .to eq([ { '_id' => 0, 'encryptedText' => 'foobarbaz' } ])
  end

  it 'Case 6: assert no document found by substring' do
    encrypted = encrypt_substring('qux', 'substring')
    result = explicit_encrypted_client['substring'].find(
      '$expr' => { '$encStrContains' => { input: '$encryptedText', substring: encrypted } }
    ).to_a
    expect(result).to be_empty
  end

  it 'Case 7: assert contentionFactor is required' do
    expect do
      client_encryption.encrypt(
        'foo',
        key_id: key1_id,
        algorithm: 'String',
        query_type: 'prefix',
        string_opts: {
          case_sensitive: true,
          diacritic_sensitive: true,
          prefix: { str_max_query_length: 10, str_min_query_length: 2 }
        }
      )
    end.to raise_error(Mongo::Error::CryptError, /contention factor is required for string algorithm/)
  end

  it 'Case 8: can find an auto-encrypted case-insensitively indexed document by prefix and suffix' do
    auto_encrypted_client['prefix-suffix-ci-di', write_concern: { w: :majority }]
      .insert_one(encryptedText: 'BingQiLin')

    prefix = encrypt_prefix_suffix_ci_di('bing', 'prefix')
    result = explicit_encrypted_client['prefix-suffix-ci-di'].find(
      '$expr' => { '$encStrStartsWith' => { input: '$encryptedText', prefix: prefix } }
    ).to_a
    expect(result.map { |doc| doc['encryptedText'] }).to eq([ 'BingQiLin' ])

    suffix = encrypt_prefix_suffix_ci_di('lin', 'suffix')
    result = explicit_encrypted_client['prefix-suffix-ci-di'].find(
      '$expr' => { '$encStrEndsWith' => { input: '$encryptedText', suffix: suffix } }
    ).to_a
    expect(result.map { |doc| doc['encryptedText'] }).to eq([ 'BingQiLin' ])
  end

  it 'Case 9: can find an auto-encrypted diacritic-insensitively indexed document by prefix and suffix' do
    auto_encrypted_client['prefix-suffix-ci-di', write_concern: { w: :majority }]
      .insert_one(encryptedText: 'cafébarbäz')

    prefix = encrypt_prefix_suffix_ci_di('cafe', 'prefix')
    result = explicit_encrypted_client['prefix-suffix-ci-di'].find(
      '$expr' => { '$encStrStartsWith' => { input: '$encryptedText', prefix: prefix } }
    ).to_a
    expect(result.map { |doc| doc['encryptedText'] }).to eq([ 'cafébarbäz' ])

    suffix = encrypt_prefix_suffix_ci_di('baz', 'suffix')
    result = explicit_encrypted_client['prefix-suffix-ci-di'].find(
      '$expr' => { '$encStrEndsWith' => { input: '$encryptedText', suffix: suffix } }
    ).to_a
    expect(result.map { |doc| doc['encryptedText'] }).to eq([ 'cafébarbäz' ])
  end

  it 'Case 10: can find an auto-encrypted case-insensitively indexed document by substring' do
    auto_encrypted_client['substring-ci-di', write_concern: { w: :majority }]
      .insert_one(encryptedText: 'FooBarBaz')

    substring = encrypt_substring_ci_di('bar', 'substring')
    result = explicit_encrypted_client['substring-ci-di'].find(
      '$expr' => { '$encStrContains' => { input: '$encryptedText', substring: substring } }
    ).to_a
    expect(result.map { |doc| doc['encryptedText'] }).to eq([ 'FooBarBaz' ])
  end

  it 'Case 11: can find an auto-encrypted diacritic-insensitively indexed document by substring' do
    auto_encrypted_client['substring-ci-di', write_concern: { w: :majority }]
      .insert_one(encryptedText: 'foocafébaz')

    substring = encrypt_substring_ci_di('cafe', 'substring')
    result = explicit_encrypted_client['substring-ci-di'].find(
      '$expr' => { '$encStrContains' => { input: '$encryptedText', substring: substring } }
    ).to_a
    expect(result.map { |doc| doc['encryptedText'] }).to eq([ 'foocafébaz' ])
  end
end
