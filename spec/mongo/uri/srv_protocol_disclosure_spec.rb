# frozen_string_literal: true

require 'lite_spec_helper'
require 'stringio'
require 'json'
require 'yaml'
require 'pp'

# Initial SRV parsing performs DNS lookups before URI.get returns, so all of
# its diagnostics are still part of the parse and must not disclose the
# connection string. These tests drive the real Mongo::Srv::Resolver and
# Mongo::Srv::Result code with a stubbed DNS client so the failure paths are
# exercised without network access.
describe Mongo::URI::SRVProtocol do
  # In this URI the first '@' delimits the userinfo, so the tail of the
  # password becomes the hostname that the driver resolves.
  let(:password_tail) { 'ssw0rdsecret' }

  let(:uri_string) { "mongodb+srv://testuser:p@#{password_tail}/db" }

  let(:dns_resolver) do
    instance_double(Resolv::DNS).tap do |dns|
      allow(dns).to receive(:timeouts=)
    end
  end

  before do
    allow(Resolv::DNS).to receive(:new).and_return(dns_resolver)
  end

  def srv_record(target, port = 27_017)
    double('srv record', target: target, port: port, ttl: 60)
  end

  def txt_record(*strings)
    double('txt record', strings: strings)
  end

  def expect_no_password_tail(text, context = nil)
    message = context ? "disclosed the password #{context}" : 'disclosed the password'
    expect(text.to_s.downcase.include?(password_tail.downcase)).to be(false), message
  end

  # Parses the URI, capturing the logger as well as anything written to the
  # process's standard streams.
  def parse_capturing(uri_string)
    log = StringIO.new
    logger = Logger.new(log)
    logger.level = Logger::DEBUG

    original_logger = Mongo::Logger.logger
    original_stdout = $stdout
    original_stderr = $stderr
    stdout = StringIO.new
    stderr = StringIO.new

    Mongo::Logger.logger = logger
    $stdout = stdout
    $stderr = stderr

    uri = nil
    error = nil
    begin
      uri = Mongo::URI.get(uri_string, logger: logger)
    rescue StandardError => e
      error = e
    end

    [ uri, error, log.string + stdout.string + stderr.string ]
  ensure
    Mongo::Logger.logger = original_logger
    $stdout = original_stdout
    $stderr = original_stderr
  end

  context 'when no SRV records are returned' do
    before do
      allow(dns_resolver).to receive(:getresources).and_return([])
    end

    it 'does not disclose the hostname in the error, its causes or the logs' do
      uri, error, logs = parse_capturing(uri_string)

      expect(uri).to be_nil
      expect(error).to be_a(Mongo::Error::NoSRVRecords)
      expect_no_password_tail(error.message, 'in the error message')
      expect_no_password_tail(error.full_message(highlight: false), 'in full_message')
      expect_no_password_tail(logs, 'in the log output')
      expect(logs).to include('attempting to resolve SRV records')
    end
  end

  context 'when the SRV record belongs to a different domain' do
    before do
      allow(dns_resolver).to receive(:getresources)
        .and_return([ srv_record('evil.attacker.tld') ])
    end

    it 'does not disclose the hostname in the error, its causes or the logs' do
      uri, error, logs = parse_capturing(uri_string)

      expect(uri).to be_nil
      expect(error).to be_a(Mongo::Error::MismatchedDomain)
      expect_no_password_tail(error.message, 'in the error message')
      expect_no_password_tail(error.full_message(highlight: false), 'in full_message')
      expect_no_password_tail(logs, 'in the log output')
    end
  end

  context 'when more than one TXT record is returned' do
    before do
      allow(dns_resolver).to receive(:getresources)
        .with("_mongodb._tcp.#{password_tail}", Resolv::DNS::Resource::IN::SRV)
        .and_return([ srv_record("seed.#{password_tail}") ])
      allow(dns_resolver).to receive(:getresources)
        .with(password_tail, Resolv::DNS::Resource::IN::TXT)
        .and_return([ txt_record('replicaSet=one'), txt_record('replicaSet=two') ])
    end

    it 'does not disclose the hostname in the error, its causes or the logs' do
      uri, error, logs = parse_capturing(uri_string)

      expect(uri).to be_nil
      expect(error).to be_a(Mongo::Error::InvalidTXTRecord)
      expect_no_password_tail(error.message, 'in the error message')
      expect_no_password_tail(error.full_message(highlight: false), 'in full_message')
      expect_no_password_tail(logs, 'in the log output')
    end
  end

  context 'when the DNS resolver raises an error' do
    before do
      allow(dns_resolver).to receive(:getresources)
        .and_raise(Resolv::ResolvError.new(
                     "DNS result has no information for _mongodb._tcp.#{password_tail}"
                   ))
    end

    it 'preserves the error class but not the unsafe message or cause' do
      uri, error, logs = parse_capturing(uri_string)

      expect(uri).to be_nil
      expect(error).to be_a(Resolv::ResolvError)
      expect_no_password_tail(error.message, 'in the error message')
      expect(error.cause).to be_nil
      expect_no_password_tail(error.full_message(highlight: false), 'in full_message')
      expect_no_password_tail(logs, 'in the log output')
    end
  end

  context 'when the DNS resolver times out' do
    before do
      allow(dns_resolver).to receive(:getresources)
        .and_raise(Resolv::ResolvTimeout.new("execution expired for #{password_tail}"))
    end

    it 'preserves the error class but not the unsafe message or cause' do
      uri, error, logs = parse_capturing(uri_string)

      expect(uri).to be_nil
      expect(error).to be_a(Resolv::ResolvTimeout)
      expect_no_password_tail(error.message, 'in the error message')
      expect(error.cause).to be_nil
      expect_no_password_tail(error.full_message(highlight: false), 'in full_message')
      expect_no_password_tail(logs, 'in the log output')
    end
  end

  context 'when the URI resolves successfully' do
    let(:user) { 'SENTINELuser5678' }
    let(:password) { 'SENTINELpassword1234' }
    let(:uri_string) do
      "mongodb+srv://#{user}:#{password}@a.example.com/db?" \
        'replicaSet=uriRs&' \
        'tlsCertificateKeyFilePassword=TLS_SECRET&' \
        'authMechanismProperties=AWS_SESSION_TOKEN:AWS_SECRET'
    end

    before do
      allow(dns_resolver).to receive(:getresources)
        .with('_mongodb._tcp.a.example.com', Resolv::DNS::Resource::IN::SRV)
        .and_return([ srv_record('seed.a.example.com') ])
      allow(dns_resolver).to receive(:getresources)
        .with('a.example.com', Resolv::DNS::Resource::IN::TXT)
        .and_return([ txt_record('authSource=txtDb') ])
    end

    it 'keeps the previous Hash shape and values' do
      options = Mongo::URI.get(uri_string).client_options

      expect(options).to be_a(Hash)

      # The key types are the same as before this change: TXT options and
      # ssl use symbol keys, URI options use string keys, and database and
      # credentials use symbol keys.
      core_keys = options.keys.select do |key|
        %i[auth_source ssl database user password replica_set].include?(key) ||
          key == 'replica_set'
      end
      expect(core_keys).to eq([ :auth_source, :ssl, 'replica_set', :database, :user, :password ])
      expect(options.key?(:ssl)).to be(true)
      expect(options.key?(:replica_set)).to be(false)
      expect(options.key?('replica_set')).to be(true)

      expect(options[:auth_source]).to eq('txtDb')
      expect(options[:ssl]).to be(true)
      expect(options['replica_set']).to eq('uriRs')
      expect(options[:database]).to eq('db')
      expect(options[:user]).to eq(user)
      expect(options[:password]).to eq(password)
      expect(options['ssl_key_pass_phrase']).to eq('TLS_SECRET')
      expect(options['auth_mech_properties'][:AWS_SESSION_TOKEN]).to eq('AWS_SECRET')

      # URI options still take precedence over TXT options once the client
      # canonicalizes the keys.
      canonical = Mongo::Client.canonicalize_ruby_options(options)
      expect(canonical[:replica_set]).to eq('uriRs')
      expect(canonical[:auth_source]).to eq('txtDb')
    end

    it 'redacts the Hash representations without changing the values' do
      options = Mongo::URI.get(uri_string).client_options

      [ options.to_s, options.inspect, options.dup.inspect ].each do |representation|
        expect(representation).not_to include(user)
        expect(representation).not_to include(password)
        expect(representation).not_to include('TLS_SECRET')
        expect(representation).not_to include('AWS_SECRET')
      end

      # One marker for the user, the password, the TLS key password and the
      # whole authMechanismProperties block, all the same.
      expect(options.inspect.scan(Mongo::Options::Redacted::STRING_REPLACEMENT).length).to eq(4)

      expect(options[:user]).to eq(user)
      expect(options[:password]).to eq(password)
    end

    it 'redacts serialized representations' do
      options = Mongo::URI.get(uri_string).client_options
      marker = Mongo::Options::Redacted::STRING_REPLACEMENT
      sentinels = [ user, password, 'TLS_SECRET', 'AWS_SECRET' ]

      json = JSON.generate({ options: options, other: 1 })
      sentinels.each do |sentinel|
        expect(json.include?(sentinel)).to be(false)
      end
      parsed = JSON.parse(json)
      expect(parsed['other']).to eq(1)
      expect(parsed['options']).to be_a(Hash)
      expect(parsed['options']['password']).to eq(marker)
      expect(parsed['options']['ssl_key_pass_phrase']).to eq(marker)
      expect(parsed['options']['auth_mech_properties']).to eq(marker)
      expect(parsed['options']['auth_source']).to eq('txtDb')
      expect(parsed['options']['ssl']).to be(true)

      [ options.to_yaml, YAML.dump(options), { options: options }.to_yaml,
        options.pretty_inspect, PP.pp(options, +''), PP.pp({ options: options }, +'') ].each do |text|
        sentinels.each do |sentinel|
          expect(text.include?(sentinel)).to be(false)
        end
      end

      expect { Utils.load_trusted_serialization_yaml(options.to_yaml) }
        .to raise_error(ArgumentError, /sensitive values were redacted/)
      expect { Utils.load_trusted_serialization_yaml({ options: options }.to_yaml) }
        .to raise_error(ArgumentError, /sensitive values were redacted/)

      dumped = Marshal.dump({ options: options })
      sentinels.each do |sentinel|
        expect(dumped.include?(sentinel)).to be(false)
      end
      # rubocop:disable Security/MarshalLoad -- the dump is created in this test
      expect { Marshal.load(dumped) }
        .to raise_error(ArgumentError, /sensitive values were redacted/)
      # rubocop:enable Security/MarshalLoad

      as_json = options.as_json
      sentinels.each do |sentinel|
        expect(JSON.generate(as_json).include?(sentinel)).to be(false)
      end
      expect(as_json[:password]).to eq(marker)

      expect(options[:user]).to eq(user)
      expect(options[:password]).to eq(password)
      expect(options['ssl_key_pass_phrase']).to eq('TLS_SECRET')
      expect(options['auth_mech_properties'][:AWS_SESSION_TOKEN]).to eq('AWS_SECRET')
    end

    it 'redacts serialized URI representations' do
      uri = Mongo::URI.get(uri_string)
      sentinels = [ user, password, 'TLS_SECRET', 'AWS_SECRET' ]

      [ uri.to_yaml, YAML.dump({ uri: uri }), JSON.generate({ uri: uri }),
        JSON.generate(uri.as_json), uri.to_extended_json, uri.pretty_inspect,
        PP.pp({ uri: uri }, +'') ].each do |text|
        sentinels.each do |sentinel|
          expect(text.include?(sentinel)).to be(false)
        end
      end

      dumped = Marshal.dump({ uri: uri })
      sentinels.each do |sentinel|
        expect(dumped.include?(sentinel)).to be(false)
      end

      yaml_loaded = Utils.load_trusted_serialization_yaml(uri.to_yaml)
      expect(yaml_loaded).to be_a(Mongo::URI::SRVProtocol)
      expect { yaml_loaded.client_options }.to raise_error(Mongo::Error::InvalidURI)
      expect { yaml_loaded.credentials }.to raise_error(Mongo::Error::InvalidURI)
      expect { yaml_loaded.servers }.to raise_error(Mongo::Error::InvalidURI)

      marshal_loaded = Marshal.load(Marshal.dump(uri))
      expect(marshal_loaded).to be_a(Mongo::URI::SRVProtocol)
      expect { marshal_loaded.client_options }.to raise_error(Mongo::Error::InvalidURI)
      expect { marshal_loaded.credentials }.to raise_error(Mongo::Error::InvalidURI)
    end

    it 'does not disclose credentials in the URI representation' do
      uri = Mongo::URI.get(uri_string)
      representation = uri.to_s

      expect(representation).not_to include(user)
      expect(representation).not_to include(password)
      expect(representation).not_to include('TLS_SECRET')
      expect(representation).not_to include('AWS_SECRET')
      expect(representation).to include('tlsCertificateKeyFilePassword=<credentials>')
      expect(representation).to include('authMechanismProperties=<credentials>')
    end
  end
end
