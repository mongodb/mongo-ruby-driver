# frozen_string_literal: true

require 'spec_helper'
require 'json'
require 'yaml'
require 'pp'

describe Mongo::Options::RedactedHash do
  let(:hash) do
    {
      user: 'SENTINELuser5678',
      'password' => 'SENTINELpassword1234',
      'ssl_key_pass_phrase' => 'TLS_SECRET',
      'auth_mech_properties' => { AWS_SESSION_TOKEN: 'AWS_SECRET' },
      ssl: true,
    }
  end

  let(:options) { described_class[hash] }

  it 'preserves the keys and their types' do
    expect(options.keys).to eq([ :user, 'password', 'ssl_key_pass_phrase', 'auth_mech_properties', :ssl ])
    expect(options.key?(:ssl)).to be(true)
    expect(options.key?('ssl')).to be(false)
    expect(options[:ssl]).to be(true)
  end

  it 'keeps the real values accessible' do
    expect(options[:user]).to eq('SENTINELuser5678')
    expect(options['password']).to eq('SENTINELpassword1234')
    expect(options['ssl_key_pass_phrase']).to eq('TLS_SECRET')
    expect(options['auth_mech_properties'][:AWS_SESSION_TOKEN]).to eq('AWS_SECRET')
  end

  it 'redacts sensitive values in to_s and inspect' do
    [ options.to_s, options.inspect, options.dup.inspect ].each do |representation|
      expect(representation).not_to include('SENTINELuser5678')
      expect(representation).not_to include('SENTINELpassword1234')
      expect(representation).not_to include('TLS_SECRET')
      expect(representation).not_to include('AWS_SECRET')
      expect(representation).to include(Mongo::Options::Redacted::STRING_REPLACEMENT)
    end
  end

  it 'is a Hash and compares to an equivalent Hash' do
    expect(options).to be_a(Hash)
    expect(options).to eq(hash)
  end

  describe 'serialized representations' do
    let(:sentinels) do
      %w[SENTINELuser5678 SENTINELpassword1234 TLS_SECRET AWS_SECRET]
    end

    let(:marker) { Mongo::Options::Redacted::STRING_REPLACEMENT }

    def expect_no_secrets(text)
      sentinels.each do |sentinel|
        expect(text.to_s.include?(sentinel)).to be(false)
      end
    end

    it 'redacts JSON and keeps non-sensitive values' do
      json = JSON.generate(options)
      expect_no_secrets(json)

      parsed = JSON.parse(json)
      expect(parsed['user']).to eq(marker)
      expect(parsed['password']).to eq(marker)
      expect(parsed['auth_mech_properties']).to eq(marker)
      expect(parsed['ssl_key_pass_phrase']).to eq(marker)
      expect(parsed['ssl']).to be(true)
    end

    it 'redacts nested JSON without changing the structure' do
      expect_no_secrets(JSON.dump(options))
      expect_no_secrets(JSON.pretty_generate(options))

      nested = JSON.parse(JSON.generate({ options: options, other: 1 }))
      expect(nested['other']).to eq(1)
      expect(nested['options']).to be_a(Hash)
      expect(nested['options']['password']).to eq(marker)
      expect(nested['options']['ssl']).to be(true)
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

    it 'round-trips options without sensitive values, preserving key types' do
      safe = described_class[app_name: 'safeApp', ssl: true]
      [ Utils.load_trusted_serialization_yaml(safe.to_yaml),
        Marshal.load(Marshal.dump(safe)) ].each do |loaded|
        expect(loaded).to be_a(described_class)
        expect(loaded.keys).to eq(safe.keys)
        expect(loaded.to_h).to eq(safe.to_h)
      end

      client = Mongo::Client.new('mongodb://localhost:27017',
                                 described_class[app_name: 'safeApp', monitoring_io: false])
      begin
        expect(client.options[:app_name]).to eq('safeApp')
      ensure
        client.close
      end
    end

    it 'treats a literal marker in an in-memory option as a real value' do
      literal = described_class[:password, Mongo::Options::Redacted::STRING_REPLACEMENT]
      expect(literal[:password]).to eq(Mongo::Options::Redacted::STRING_REPLACEMENT)

      client = Mongo::Client.new('mongodb://localhost:27017', literal.merge(monitoring_io: false))
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
      expect(parsed['ssl']).to be(true)

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
      expect(parsed['options']['ssl']).to be(true)
    end

    it 'redacts as_json' do
      as_json = options.as_json
      expect_no_secrets(JSON.generate(as_json))
      expect(as_json['password']).to eq(marker)
    end

    it 'refuses legacy payloads without the redaction envelope' do
      legacy_yaml = "--- !ruby/object:Mongo::Options::RedactedHash\npassword: secret\n"
      expect { Utils.load_trusted_serialization_yaml(legacy_yaml) }
        .to raise_error(ArgumentError, /without a redaction envelope/)

      expect { described_class.allocate.marshal_load('password' => 'secret') }
        .to raise_error(ArgumentError, /without a redaction envelope/)
    end
  end
end
