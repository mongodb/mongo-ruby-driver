# frozen_string_literal: true

require 'spec_helper'
require 'json'
require 'yaml'
require 'pp'

describe Mongo::Options::Redacted do
  let(:options) do
    described_class.new(original_opts)
  end

  describe '#to_s' do
    context 'when the hash contains a sensitive key' do
      let(:original_opts) do
        { password: 'sensitive_data' }
      end

      it 'replaces the value with the redacted string' do
        expect(options.to_s).not_to match(original_opts[:password])
      end

      it 'replaces the value with the redacted string' do
        expect(options.to_s).to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end

    context 'when the hash does not contain a sensitive key' do
      let(:original_opts) do
        { name: 'emily' }
      end

      it 'prints all the values' do
        expect(options.to_s).to match(original_opts[:name])
      end
    end

    context 'when the hash contains the username component of the userinfo' do
      let(:original_opts) do
        { user: 'SENTINELuser5678' }
      end

      it 'replaces the username with the redacted string' do
        expect(options.to_s).not_to include('SENTINELuser5678')
        expect(options.inspect).not_to include('SENTINELuser5678')
        expect(options.inspect).to include(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end

    context 'when the hash contains sensitive URI option values' do
      let(:original_opts) do
        {
          ssl_key_pass_phrase: 'SENTINELpassword1234',
          auth_mech_properties: { AWS_SESSION_TOKEN: 'SENTINELpassword1234' },
        }
      end

      it 'does not print the TLS key password' do
        expect(options.to_s).not_to include('SENTINELpassword1234')
      end

      it 'does not print auth mechanism properties' do
        expect(options.inspect).not_to include('SENTINELpassword1234')
        expect(options.inspect).not_to include('AWS_SESSION_TOKEN')
      end
    end
  end

  describe '#inspect' do
    context 'when the hash contains a sensitive key' do
      let(:original_opts) do
        { password: 'sensitive_data' }
      end

      it 'replaces the value with the redacted string' do
        expect(options.inspect).not_to match(original_opts[:password])
      end

      it 'replaces the value with the redacted string' do
        expect(options.inspect).to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end

    context 'when the hash does not contain a sensitive key' do
      let(:original_opts) do
        { name: 'some_name' }
      end

      it 'does not replace the value with the redacted string' do
        expect(options.inspect).to match(original_opts[:name])
      end

      it 'does not replace the value with the redacted string' do
        expect(options.inspect).not_to match(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end
    end
  end

  describe '#has_key?' do
    context 'when the original key is a String' do
      let(:original_opts) do
        { 'name' => 'Emily' }
      end

      context 'when the method argument is a String' do
        it 'returns true' do
          expect(options.has_key?('name')).to be(true)
        end
      end

      context 'when method argument is a Symbol' do
        it 'returns true' do
          expect(options.has_key?(:name)).to be(true)
        end
      end
    end

    context 'when the original key is a Symbol' do
      let(:original_opts) do
        { name: 'Emily' }
      end

      context 'when the method argument is a String' do
        it 'returns true' do
          expect(options.has_key?('name')).to be(true)
        end
      end

      context 'when method argument is a Symbol' do
        it 'returns true' do
          expect(options.has_key?(:name)).to be(true)
        end
      end
    end

    context 'when the hash does not contain the key' do
      let(:original_opts) do
        { other: 'Emily' }
      end

      context 'when the method argument is a String' do
        it 'returns false' do
          expect(options.has_key?('name')).to be(false)
        end
      end

      context 'when method argument is a Symbol' do
        it 'returns false' do
          expect(options.has_key?(:name)).to be(false)
        end
      end
    end
  end

  describe '#reject' do
    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do
      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do
      context 'when the block evaluates to true for some pairs' do
        let(:result) do
          options.reject { |k, _v| k == 'a' }
        end

        it 'returns an object consisting of only the remaining pairs' do
          expect(result).to eq(described_class.new(b: 2, c: 3))
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do
        let(:result) do
          options.reject { |k, _v| k == 'd' }
        end

        it 'returns an object with all pairs intact' do
          expect(result).to eq(described_class.new(a: 1, b: 2, c: 3))
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end
    end
  end

  describe '#reject!' do
    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do
      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do
      context 'when the block evaluates to true for some pairs' do
        let(:result) do
          options.reject! { |k, _v| k == 'a' }
        end

        it 'returns an object consisting of only the remaining pairs' do
          expect(result).to eq(described_class.new(b: 2, c: 3))
        end

        it 'returns the same object' do
          expect(result).to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do
        let(:result) do
          options.reject! { |k, _v| k == 'd' }
        end

        it 'returns nil' do
          expect(result).to be_nil
        end
      end
    end
  end

  describe '#select' do
    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do
      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do
      context 'when the block evaluates to true for some pairs' do
        let(:result) do
          options.select { |k, _v| k == 'a' }
        end

        it 'returns an object consisting of those pairs' do
          expect(result).to eq(described_class.new(a: 1))
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do
        let(:result) do
          options.select { |k, _v| k == 'd' }
        end

        it 'returns an object with no pairs' do
          expect(result).to eq(described_class.new)
        end

        it 'returns a new object' do
          expect(result).not_to be(options)
        end
      end

      context 'when the object is unchanged' do
        let(:options) do
          described_class.new(a: 1, b: 2, c: 3)
        end

        let(:result) do
          options.select { |k, _v| %w[a b c].include?(k) }
        end

        it 'returns a new object' do
          expect(result).to eq(described_class.new(a: 1, b: 2, c: 3))
        end
      end
    end
  end

  describe '#select!' do
    let(:options) do
      described_class.new(a: 1, b: 2, c: 3)
    end

    context 'when no block is provided' do
      it 'returns an enumerable' do
        expect(options.reject).to be_a(Enumerator)
      end
    end

    context 'when a block is provided' do
      context 'when the block evaluates to true for some pairs' do
        let(:result) do
          options.select! { |k, _v| k == 'a' }
        end

        it 'returns an object consisting of those pairs' do
          expect(result).to eq(described_class.new(a: 1))
        end

        it 'returns the same object' do
          expect(result).to be(options)
        end
      end

      context 'when the block does not evaluate to true for any pairs' do
        let(:result) do
          options.select! { |k, _v| k == 'd' }
        end

        it 'returns an object with no pairs' do
          expect(result).to eq(described_class.new)
        end

        it 'returns the same object' do
          expect(result).to be(options)
        end
      end

      context 'when the object is unchanged' do
        let(:options) do
          described_class.new(a: 1, b: 2, c: 3)
        end

        let(:result) do
          options.select! { |k, _v| %w[a b c].include?(k) }
        end

        it 'returns nil' do
          expect(result).to be_nil
        end
      end
    end
  end

  describe 'serialized representations' do
    let(:sentinels) do
      %w[SENTINELuser5678 SENTINELpassword1234 TLS_SECRET AWS_SECRET]
    end

    let(:marker) { Mongo::Options::Redacted::STRING_REPLACEMENT }

    let(:original_opts) do
      {
        user: 'SENTINELuser5678',
        password: 'SENTINELpassword1234',
        auth_mech_properties: { AWS_SESSION_TOKEN: 'AWS_SECRET' },
        ssl_key_pass_phrase: 'TLS_SECRET',
        app_name: 'visibleApp',
      }
    end

    def expect_no_secrets(text)
      sentinels.each do |sentinel|
        expect(text.to_s.include?(sentinel)).to be(false)
      end
    end

    it 'redacts JSON' do
      json = JSON.generate(options)
      expect_no_secrets(json)

      parsed = JSON.parse(json)
      expect(parsed['user']).to eq(marker)
      expect(parsed['password']).to eq(marker)
      expect(parsed['auth_mech_properties']).to eq(marker)
      expect(parsed['ssl_key_pass_phrase']).to eq(marker)
      expect(parsed['app_name']).to eq('visibleApp')
    end

    it 'redacts JSON.dump, JSON.pretty_generate and nested JSON without changing the structure' do
      expect_no_secrets(JSON.dump(options))
      expect_no_secrets(JSON.pretty_generate(options))

      nested = JSON.parse(JSON.generate({ options: options, other: 1 }))
      expect(nested['other']).to eq(1)
      expect(nested['options']).to be_a(Hash)
      expect(nested['options']['password']).to eq(marker)
      expect(nested['options']['app_name']).to eq('visibleApp')
    end

    it 'redacts YAML and refuses to deserialize redacted credentials' do
      [ options.to_yaml, YAML.dump(options), { options: options }.to_yaml ].each do |yaml|
        expect_no_secrets(yaml)
        expect(yaml).to include(marker)
      end

      expect { Utils.load_trusted_serialization_yaml(options.to_yaml) }
        .to raise_error(ArgumentError, /sensitive values were redacted/)
      expect { Utils.load_trusted_serialization_yaml({ options: options }.to_yaml) }
        .to raise_error(ArgumentError, /sensitive values were redacted/)
    end

    it 'redacts pretty printed output, including nested output' do
      [ options.pretty_inspect, PP.pp(options, +''), PP.pp({ options: options }, +'') ].each do |text|
        expect_no_secrets(text)
      end

      expect(options.pretty_inspect).to include(marker)
    end

    it 'redacts Marshal output and refuses to deserialize redacted credentials' do
      dumped = Marshal.dump({ options: options })
      expect_no_secrets(dumped)

      # rubocop:disable Security/MarshalLoad -- the dump is created in this test
      expect { Marshal.load(dumped) }
        .to raise_error(ArgumentError, /sensitive values were redacted/)
      # rubocop:enable Security/MarshalLoad
    end

    it 'round-trips options without sensitive values' do
      safe = described_class.new(app_name: 'safeApp', 'custom' => 'value')
      [ Utils.load_trusted_serialization_yaml(safe.to_yaml),
        Marshal.load(Marshal.dump(safe)) ].each do |loaded|
        expect(loaded).to be_a(described_class)
        expect(loaded.keys).to eq(safe.keys)
        expect(loaded.to_h).to eq(safe.to_h)
      end

      client = Mongo::Client.new('mongodb://localhost:27017',
                                 described_class.new(app_name: 'safeApp', monitoring_io: false))
      begin
        expect(client.options[:app_name]).to eq('safeApp')
      ensure
        client.close
      end
    end

    it 'treats a literal marker in an in-memory option as a real value' do
      literal = described_class.new(password: Mongo::Options::Redacted::STRING_REPLACEMENT,
                                    monitoring_io: false)
      expect(literal['password']).to eq(Mongo::Options::Redacted::STRING_REPLACEMENT)

      client = Mongo::Client.new('mongodb://localhost:27017', literal)
      begin
        expect(client.options[:password]).to eq(Mongo::Options::Redacted::STRING_REPLACEMENT)
      ensure
        client.close
      end
    end

    it 'redacts Extended JSON' do
      extended = options.to_extended_json
      expect_no_secrets(extended)

      parsed = JSON.parse(extended)
      expect(parsed['password']).to eq(marker)
      expect(parsed['user']).to eq(marker)
      expect(parsed['auth_mech_properties']).to eq(marker)
      expect(parsed['ssl_key_pass_phrase']).to eq(marker)
      expect(parsed['app_name']).to eq('visibleApp')

      as_extended = options.as_extended_json
      expect_no_secrets(JSON.generate(as_extended))
      expect(as_extended['password']).to eq(marker)
    end

    it 'redacts nested Extended JSON without changing the structure' do
      extended = { options: options, other: 'value' }.to_extended_json
      expect_no_secrets(extended)

      parsed = JSON.parse(extended)
      expect(parsed['other']).to eq('value')
      expect(parsed['options']).to be_a(Hash)
      expect(parsed['options']['password']).to eq(marker)
      expect(parsed['options']['app_name']).to eq('visibleApp')
    end

    it 'redacts as_json' do
      as_json = options.as_json
      expect_no_secrets(JSON.generate(as_json))
      expect(as_json['password']).to eq(marker)
    end

    it 'refuses legacy payloads without the redaction envelope' do
      legacy_yaml = "--- !ruby/object:Mongo::Options::Redacted\npassword: secret\n"
      expect { Utils.load_trusted_serialization_yaml(legacy_yaml) }
        .to raise_error(ArgumentError, /without a redaction envelope/)

      expect { described_class.allocate.marshal_load('password' => 'secret') }
        .to raise_error(ArgumentError, /without a redaction envelope/)
    end

    it 'does not change the stored values' do
      expect(options['user']).to eq('SENTINELuser5678')
      expect(options['password']).to eq('SENTINELpassword1234')
      expect(options['auth_mech_properties'][:AWS_SESSION_TOKEN]).to eq('AWS_SECRET')
      expect(options['ssl_key_pass_phrase']).to eq('TLS_SECRET')
      expect(options['app_name']).to eq('visibleApp')
    end
  end
end
