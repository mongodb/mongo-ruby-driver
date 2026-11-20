# frozen_string_literal: true

require 'lite_spec_helper'
require 'stringio'
require 'open3'
require 'rbconfig'
require 'json'
require 'yaml'
require 'pp'

# Tests for the "Disclosure of Sensitive Values" section of the connection
# string specification.
#
# The corpus lives in its own directory on purpose: it has a different shape
# from the connection string corpus (no "valid"/"options" keys), so it must
# not be picked up by the globs in lite_spec_helper.
describe 'Credential disclosure' do
  credential_disclosure_tests = Utils.load_spec_yaml_file(
    File.join(CURRENT_PATH, 'spec_tests/data/credential_disclosure/credential-disclosure.yml')
  ).fetch('tests')

  # The sentinel credential used by the corpus and the prose tests, and the
  # same sentinel fully percent-encoded. Both forms must be searched for,
  # case-insensitively.
  let(:literal_sentinel) { 'SENTINELpassword1234' }

  # Stubs the lowest DNS layer, so the real Mongo::Srv::Resolver and
  # Mongo::Srv::Result code runs. SRV URIs resolve to no records, which keeps
  # the corpus network-free and exercises the SRV failure diagnostics.
  let(:dns_resolver) do
    instance_double(Resolv::DNS).tap do |dns|
      allow(dns).to receive(:timeouts=)
      allow(dns).to receive(:getresources).and_return([])
    end
  end

  def expect_no_sentinel(text, context = nil, sentinels: [ literal_sentinel ])
    downcased = text.to_s.downcase
    message = context ? "disclosed a sentinel #{context}" : 'disclosed a sentinel'

    sentinels.each do |sentinel|
      encoded = sentinel.bytes.map { |byte| format('%%%02X', byte) }.join
      # Use boolean assertions so a failure does not echo the offending
      # message, which would print the credential into the test log.
      expect(downcased.include?(sentinel.downcase)).to be(false), message
      expect(downcased.include?(encoded.downcase)).to be(false), message
    end
  end

  # Parses the URI, capturing everything the driver logs at DEBUG level, and
  # returns the error it raised (or nil if the URI was accepted) together with
  # the log output.
  def parse_capturing_logs(uri_string)
    log = StringIO.new
    logger = Logger.new(log)
    logger.level = Logger::DEBUG

    original_logger = Mongo::Logger.logger
    Mongo::Logger.logger = logger

    error = begin
      Mongo::URI.get(uri_string, logger: logger)
      nil
    rescue StandardError => e
      e
    end

    [ error, log.string ]
  ensure
    Mongo::Logger.logger = original_logger
  end

  before do
    allow(Resolv::DNS).to receive(:new).and_return(dns_resolver)
  end

  credential_disclosure_tests.each do |test|
    context test.fetch('description') do
      let(:uri_string) { test.fetch('uri') }

      it 'does not disclose a sentinel in the error message or in a cause' do
        error, = parse_capturing_logs(uri_string)

        if error
          expect_no_sentinel(error.message, 'in the error message')

          cause = error.cause
          while cause
            expect_no_sentinel(cause.message, "in the cause #{cause.class}")
            cause = cause.cause
          end
        end
      end

      it 'does not disclose a sentinel in messages logged while parsing' do
        _, logs = parse_capturing_logs(uri_string)

        expect_no_sentinel(logs, 'in the log output')
      end

      it 'does not disclose a sentinel when the error is not handled' do
        error, = parse_capturing_logs(uri_string)

        if error
          # This is what the Ruby runtime's default handler prints for an
          # unhandled exception: the message, the cause chain and the
          # backtraces.
          expect_no_sentinel(error.full_message(highlight: false), 'in full_message')
        end
      end
    end
  end

  describe 'unhandled parse errors in a subprocess' do
    let(:lib_dir) { File.expand_path('../../lib', __dir__) }

    # Ordinary URI shapes that a user is likely to feed to a short script.
    # Each one fails before any DNS lookup would be attempted.
    let(:uris) do
      [
        'mongodb://alice:TOP_SECRET',
        "mongodb://alice:#{literal_sentinel}",
        "mongodb://testuser:#{literal_sentinel}@localhost:999999/admin",
        "mongod://testuser:#{literal_sentinel}@localhost:27017/admin",
        "mongodb://testuser:#{literal_sentinel}/admin?tlsCertificateKeyFilePassword=#{literal_sentinel}",
      ]
    end

    it 'writes nothing derived from the connection string' do
      uris.each do |uri|
        stdout, stderr, status = Open3.capture3(
          RbConfig.ruby,
          '-I', lib_dir,
          '-e', "require 'mongo'; Mongo::URI.get(ARGV[0])",
          '--', uri
        )

        expect(status).not_to be_success
        # Guard against the test passing because the process failed early,
        # for example with LoadError while loading the driver.
        expect(stderr).not_to include('LoadError')
        expect(stderr).to include('Mongo::Error::InvalidURI')
        expect_no_sentinel(stdout + stderr, 'on the default unhandled error path')
      end
    end
  end

  describe 'prose tests' do
    let(:user_sentinel) { 'SENTINELuser5678' }

    let(:password) { 'SENTINELpassword1234' }

    let(:uri_string) do
      "mongodb://#{user_sentinel}:#{password}@localhost:27017/db"
    end

    let(:uri) { Mongo::URI.get(uri_string) }

    let(:prose_sentinels) { [ user_sentinel, password ] }

    context 'redacted string representations' do
      it 'does not disclose the credentials in the URI representations' do
        expect_no_sentinel(uri.to_s, 'in URI#to_s', sentinels: prose_sentinels)
        expect_no_sentinel(uri.inspect, 'in URI#inspect', sentinels: prose_sentinels)
        expect(uri.to_s).to include(Mongo::URI::CREDENTIALS_PLACEHOLDER)
      end

      it 'does not disclose the credentials in the options representations' do
        expect_no_sentinel(uri.uri_options.to_s, 'in uri_options#to_s', sentinels: prose_sentinels)
        expect_no_sentinel(uri.uri_options.inspect, 'in uri_options#inspect', sentinels: prose_sentinels)
        expect_no_sentinel(uri.client_options.to_s, 'in client_options#to_s', sentinels: prose_sentinels)
        expect_no_sentinel(uri.client_options.inspect, 'in client_options#inspect', sentinels: prose_sentinels)
        expect(uri.client_options.inspect)
          .to include(Mongo::Options::Redacted::STRING_REPLACEMENT)
      end

      it 'does not disclose the credentials in the client options representation' do
        client = Mongo::Client.new(uri_string, monitoring_io: false)

        begin
          expect_no_sentinel(client.options.to_s, 'in client options#to_s', sentinels: prose_sentinels)
          expect_no_sentinel(client.options.inspect, 'in client options#inspect', sentinels: prose_sentinels)
        ensure
          client.close
        end
      end
    end

    context 'sensitive URI options' do
      let(:uri_string) do
        'mongodb://localhost:27017/db?tlsCertificateKeyFilePassword=TLS_SECRET&' \
          'authMechanismProperties=AWS_SESSION_TOKEN:AWS_SECRET'
      end

      it 'does not disclose their arguments in any representation' do
        expect(uri.to_s).not_to include('TLS_SECRET')
        expect(uri.to_s).not_to include('AWS_SECRET')
        expect(uri.inspect).not_to include('TLS_SECRET')
        expect(uri.inspect).not_to include('AWS_SECRET')
        expect(uri.uri_options.to_s).not_to include('TLS_SECRET')
        expect(uri.uri_options.to_s).not_to include('AWS_SECRET')
        expect(uri.uri_options.inspect).not_to include('TLS_SECRET')
        expect(uri.uri_options.inspect).not_to include('AWS_SECRET')
        expect(uri.client_options.to_s).not_to include('TLS_SECRET')
        expect(uri.client_options.to_s).not_to include('AWS_SECRET')
      end

      it 'keeps the real values for connecting' do
        expect(uri.uri_options[:ssl_key_pass_phrase]).to eq('TLS_SECRET')
        expect(uri.uri_options[:auth_mech_properties][:AWS_SESSION_TOKEN])
          .to eq('AWS_SECRET')
        expect(uri.client_options[:ssl_key_pass_phrase]).to eq('TLS_SECRET')
      end
    end

    context 'serialized representations' do
      let(:uri_string) do
        "mongodb://#{user_sentinel}:#{password}@localhost:27017/db?" \
          'appName=visibleApp&' \
          'tlsCertificateKeyFilePassword=TLS_SECRET&' \
          'authMechanismProperties=AWS_SESSION_TOKEN:AWS_SECRET'
      end

      let(:sentinels) { [ user_sentinel, password, 'TLS_SECRET', 'AWS_SECRET' ] }

      let(:marker) { Mongo::Options::Redacted::STRING_REPLACEMENT }

      let(:option_objects) { [ uri.uri_options, uri.client_options ] }

      it 'redacts JSON, including JSON.dump, pretty and nested output' do
        option_objects.each do |opts|
          [
            JSON.generate(opts),
            JSON.dump(opts),
            JSON.pretty_generate(opts),
            JSON.generate({ options: opts, other: 1 }),
            JSON.generate([ opts ]),
          ].each do |text|
            expect_no_sentinel(text, 'in a JSON representation', sentinels: sentinels)
          end

          parsed = JSON.parse(JSON.generate(opts))
          expect(parsed['app_name']).to eq('visibleApp')
          expect(parsed['ssl_key_pass_phrase']).to eq(marker)
          expect(parsed['auth_mech_properties']).to eq(marker)
          expect(parsed['user']).to eq(marker) if parsed.key?('user')
          expect(parsed['password']).to eq(marker) if parsed.key?('password')

          nested = JSON.parse(JSON.generate({ options: opts, other: 1 }))
          expect(nested['other']).to eq(1)
          expect(nested['options']).to be_a(Hash)
          expect(nested['options']['app_name']).to eq('visibleApp')
        end
      end

      it 'redacts YAML and pretty printed output, including nested output' do
        option_objects.each do |opts|
          [ opts.to_yaml, YAML.dump(opts), { options: opts }.to_yaml ].each do |text|
            expect_no_sentinel(text, 'in a YAML representation', sentinels: sentinels)
          end

          expect { Utils.load_trusted_serialization_yaml(opts.to_yaml) }
            .to raise_error(ArgumentError, /sensitive values were redacted/)
          expect { Utils.load_trusted_serialization_yaml({ options: opts }.to_yaml) }
            .to raise_error(ArgumentError, /sensitive values were redacted/)

          [ opts.pretty_inspect, PP.pp(opts, +''), PP.pp({ options: opts }, +'') ].each do |text|
            expect_no_sentinel(text, 'in pretty printed output', sentinels: sentinels)
          end
        end
      end

      it 'redacts Marshal output and round-trips a redacted object' do
        option_objects.each do |opts|
          dumped = Marshal.dump({ options: opts })
          expect_no_sentinel(dumped, 'in Marshal output', sentinels: sentinels)

          # rubocop:disable Security/MarshalLoad -- the dump is created in this test
          expect { Marshal.load(dumped) }
            .to raise_error(ArgumentError, /sensitive values were redacted/)
          # rubocop:enable Security/MarshalLoad
        end
      end

      it 'redacts Extended JSON' do
        option_objects.each do |opts|
          extended = opts.to_extended_json
          expect_no_sentinel(extended, 'in Extended JSON', sentinels: sentinels)

          parsed = JSON.parse(extended)
          expect(parsed['app_name']).to eq('visibleApp')
          expect(parsed['ssl_key_pass_phrase']).to eq(marker)
          expect(parsed['auth_mech_properties']).to eq(marker)
          expect(parsed['user']).to eq(marker) if parsed.key?('user')
          expect(parsed['password']).to eq(marker) if parsed.key?('password')

          as_extended = opts.as_extended_json
          expect_no_sentinel(JSON.generate(as_extended), 'in as_extended_json', sentinels: sentinels)
          expect(as_extended['ssl_key_pass_phrase']).to eq(marker)

          nested = JSON.parse({ options: opts }.to_extended_json)
          expect(nested['options']).to be_a(Hash)
          expect(nested['options']['app_name']).to eq('visibleApp')
        end
      end

      it 'redacts as_json' do
        option_objects.each do |opts|
          as_json = opts.as_json
          expect_no_sentinel(JSON.generate(as_json), 'in as_json', sentinels: sentinels)
          expect(as_json['ssl_key_pass_phrase']).to eq(marker)
        end
      end

      it 'redacts Mongo::Client#options serialization' do
        client = Mongo::Client.new(uri_string, monitoring_io: false)

        begin
          options = client.options

          [
            JSON.generate({ options: options }),
            JSON.dump({ options: options }),
            JSON.pretty_generate({ options: options }),
            { options: options }.to_yaml,
            PP.pp({ options: options }, +''),
            Marshal.dump({ options: options }),
          ].each do |text|
            expect_no_sentinel(text, 'in a client options representation', sentinels: sentinels)
          end

          parsed = JSON.parse(JSON.generate({ options: options }))
          expect(parsed['options']['password']).to eq(marker)
          expect(parsed['options']['ssl_key_pass_phrase']).to eq(marker)
          expect(parsed['options']['app_name']).to eq('visibleApp')
        ensure
          client.close
        end
      end

      it 'does not change the real values used for connecting' do
        expect(uri.credentials[:password]).to eq(password)
        expect(uri.client_options[:password]).to eq(password)
        expect(uri.client_options[:ssl_key_pass_phrase]).to eq('TLS_SECRET')
        expect(uri.uri_options['ssl_key_pass_phrase']).to eq('TLS_SECRET')
      end
    end

    context 'uniform redaction marker' do
      let(:uri_string) do
        "mongodb://#{user_sentinel}:#{password}@localhost:27017/db?" \
          'tlsCertificateKeyFilePassword=TLS_SECRET&' \
          'authMechanismProperties=AWS_SESSION_TOKEN:AWS_SECRET'
      end

      it 'uses the same marker for the userinfo and every sensitive option' do
        representation = uri.to_s

        expect_no_sentinel(
          representation,
          'in URI#to_s',
          sentinels: prose_sentinels + %w[TLS_SECRET AWS_SECRET]
        )
        expect(representation.scan(Mongo::URI::CREDENTIALS_PLACEHOLDER).length).to eq(3)
        expect(representation).not_to include('<redacted>')
      end
    end

    context 'explicitly named unredacted API' do
      it 'returns the real credentials from #credentials' do
        expect(uri.credentials[:user]).to eq(user_sentinel)
        expect(uri.credentials[:password]).to eq(password)
      end
    end

    context 'canonical authentication mechanism diagnostic' do
      it 'does not include an unrecognized mechanism argument' do
        expect do
          Mongo::Client.new(
            "mongodb://user:password@localhost:27017/?authMechanism=#{literal_sentinel}",
            monitoring_io: false
          )
        end.to raise_error(Mongo::Auth::InvalidMechanism) { |error|
          expect_no_sentinel(error.message, 'in the mechanism diagnostic')
        }
      end

      it 'identifies a recognized mechanism by its canonical name' do
        expect do
          Mongo::Client.new(
            'mongodb://user:password@localhost:27017/?authMechanism=mOnGoDb-X509&authSource=foo',
            monitoring_io: false
          )
        end.to raise_error(Mongo::Auth::InvalidConfiguration) { |error|
          # The driver identifies the mechanism by its own canonical symbol,
          # not by the spelling supplied in the connection string.
          expect(error.message).to include('mongodb_x509')
          expect(error.message).not_to include('mOnGoDb-X509')
        }
      end
    end

    context 'parsed integer diagnostic' do
      it 'formats the parsed integer rather than the supplied spelling' do
        expect do
          Mongo::Client.new(
            'mongodb://localhost:27017/?minPoolSize=0005&maxPoolSize=0002',
            monitoring_io: false
          )
        end.to raise_error(Mongo::Error::InvalidMinPoolSize) { |error|
          expect(error.message).to match(/Invalid min pool size: 5\./)
          expect(error.message).to match(/max size: 2\./)
          expect(error.message).not_to include('0005')
          expect(error.message).not_to include('0002')
        }
      end

      it 'formats a percent-encoded parsed integer rather than the supplied spelling' do
        expect do
          Mongo::Client.new(
            'mongodb://localhost:27017/?minPoolSize=%30%30%30%35&maxPoolSize=%30%30%30%32',
            monitoring_io: false
          )
        end.to raise_error(Mongo::Error::InvalidMinPoolSize) { |error|
          expect(error.message).to match(/Invalid min pool size: 5\./)
          expect(error.message).not_to include('%30')
        }
      end
    end

    # Prose test 5 (representation of a connection string that did not parse)
    # does not apply: a Mongo::URI object cannot exist unless parsing
    # succeeded, so there is no representation to inspect.
  end
end
