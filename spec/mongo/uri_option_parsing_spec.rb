# frozen_string_literal: true
# rubocop:todo all

require 'lite_spec_helper'

describe Mongo::URI do

  let(:uri) { described_class.new(string) }

  shared_examples_for 'parses successfully' do
    it 'returns a Mongo::URI object' do
      expect(uri).to be_a(Mongo::URI)
    end
  end

  shared_examples_for 'raises parse error' do
    it 'raises InvalidURI' do
      expect do
        uri
      end.to raise_error(Mongo::Error::InvalidURI)
    end
  end

  shared_examples_for 'a millisecond option' do

    let(:string) { "mongodb://example.com/?#{uri_option}=123" }

    it_behaves_like 'parses successfully'

    it 'is a float' do
      expect(uri.uri_options[ruby_option]).to eq(0.123)
    end

    context 'a multiple of 1 second' do
      let(:string) { "mongodb://example.com/?#{uri_option}=123000" }

      it_behaves_like 'parses successfully'

      it 'is a float' do
        expect(uri.uri_options[ruby_option]).to be_a(Float)
        expect(uri.uri_options[ruby_option]).to eq(123)
      end
    end
  end

  shared_examples_for 'an integer option' do

    let(:string) { "mongodb://example.com/?#{uri_option}=123" }

    it_behaves_like 'parses successfully'

    it 'is an integer' do
      expect(uri.uri_options[ruby_option]).to eq(123)
    end

    context 'URL encoded value' do
      let(:string) { "mongodb://example.com/?#{uri_option}=%31%32%33" }

      it_behaves_like 'parses successfully'

      it 'is an integer' do
        expect(uri.uri_options[ruby_option]).to eq(123)
      end
    end
  end

  shared_examples_for 'a boolean option' do

    context 'is true' do

      let(:string) { "mongodb://example.com/?#{uri_option}=true" }

      it_behaves_like 'parses successfully'

      it 'is a boolean' do
        expect(uri.uri_options[ruby_option]).to be true
      end
    end

    context 'is TRUE' do

      let(:string) { "mongodb://example.com/?#{uri_option}=TRUE" }

      it_behaves_like 'parses successfully'

      it 'is a boolean' do
        expect(uri.uri_options[ruby_option]).to be true
      end
    end

    context 'is false' do

      let(:string) { "mongodb://example.com/?#{uri_option}=false" }

      it_behaves_like 'parses successfully'

      it 'is a boolean' do
        expect(uri.uri_options[ruby_option]).to be false
      end
    end

    context 'is FALSE' do

      let(:string) { "mongodb://example.com/?#{uri_option}=FALSE" }

      it_behaves_like 'parses successfully'

      it 'is a boolean' do
        expect(uri.uri_options[ruby_option]).to be false
      end
    end
  end

  shared_examples_for 'an inverted boolean option' do

    let(:string) { "mongodb://example.com/?#{uri_option}=true" }

    it_behaves_like 'parses successfully'

    it 'is a boolean' do
      expect(uri.uri_options[ruby_option]).to be false
    end
  end

  shared_examples_for 'a string option' do

    let(:string) { "mongodb://example.com/?#{uri_option}=foo" }

    it_behaves_like 'parses successfully'

    it 'is a string' do
      expect(uri.uri_options[ruby_option]).to eq('foo')
    end

    context 'it is escaped in URI' do
      let(:string) { "mongodb://example.com/?#{uri_option}=foo%2f" }

      it 'is unescaped' do
        expect(uri.uri_options[ruby_option]).to eq('foo/')
      end
    end

    context 'it is escaped twice in URI' do
      let(:string) { "mongodb://example.com/?#{uri_option}=foo%252f" }

      it 'is unescaped once' do
        expect(uri.uri_options[ruby_option]).to eq('foo%2f')
      end
    end

    context 'value is a number' do
      let(:string) { "mongodb://example.com/?#{uri_option}=1" }

      it_behaves_like 'parses successfully'

      it 'is a string' do
        expect(uri.uri_options[ruby_option]).to eq('1')
      end
    end
  end

  context 'appName' do

    let(:uri_option) { 'appName' }
    let(:ruby_option) { :app_name }

    it_behaves_like 'a string option'
  end

  context 'authMechanism' do

    let(:string) { 'mongodb://example.com/?authMechanism=SCRAM-SHA-256' }

    it_behaves_like 'parses successfully'

    it 'is a symbol' do
      expect(uri.uri_options[:auth_mech]).to eq(:scram256)
    end

    context 'lowercase value' do

      let(:string) { 'mongodb://example.com/?authMechanism=scram-sha-256' }

      it_behaves_like 'parses successfully'

      it 'is mapped to auth mechanism' do
        expect(uri.uri_options[:auth_mech]).to eq(:scram256)
      end
    end

    context 'unrecognized value' do

      let(:string) { 'mongodb://example.com/?authMechanism=foobar' }

      it_behaves_like 'parses successfully'

      it 'is mapped to auth mechanism' do
        expect(uri.uri_options[:auth_mech]).to eq('foobar')
      end
    end
  end

  context 'authMechanismProperties' do

    let(:string) { 'mongodb://example.com/?authmechanismproperties=SERVICE_realm:foo,CANONICALIZE_HOST_name:TRUE' }

    it_behaves_like 'parses successfully'

    it 'parses correctly' do
      expect(uri.uri_options[:auth_mech_properties]).to eq(BSON::Document.new(
        SERVICE_realm: 'foo',
        CANONICALIZE_HOST_name: true,
      ))
    end

    context 'canonicalize host name is false' do

      let(:string) { 'mongodb://example.com/?authmechanismproperties=SERVICE_realm:foo,CANONICALIZE_HOST_name:false' }

      it 'parses correctly' do
        expect(uri.uri_options[:auth_mech_properties]).to eq(BSON::Document.new(
          SERVICE_realm: 'foo',
          CANONICALIZE_HOST_name: false,
        ))
      end
    end

    context 'canonicalize host name is true in mixed case' do

      let(:string) { 'mongodb://example.com/?authmechanismproperties=SERVICE_realm:foo,CANONICALIZE_HOST_name:TrUe' }

      it 'parses correctly' do
        expect(uri.uri_options[:auth_mech_properties]).to eq(BSON::Document.new(
          SERVICE_realm: 'foo',
          CANONICALIZE_HOST_name: true,
        ))
      end
    end
  end

  context 'authSource' do

    let(:uri_option) { 'authSource' }
    let(:ruby_option) { :auth_source }

    it_behaves_like 'a string option'

    context 'empty' do
      let(:string) { "mongodb://example.com/?#{uri_option}=" }

      it 'is mapped to the empty string' do
        expect(uri.uri_options[ruby_option]).to eq('')
      end
    end
  end

  context 'compressors' do

    let(:string) { 'mongodb://example.com/?compressors=snappy,zlib' }

    it_behaves_like 'parses successfully'

    it 'is an array of strings string' do
      expect(uri.uri_options[:compressors]).to eq(['snappy', 'zlib'])
    end
  end

  context 'connect' do

    let(:client) { new_local_client_nmio(string) }

    shared_examples 'raises an error when client is created' do
      it 'raises an error when client is created' do
        lambda do
          client
        end.should raise_error(ArgumentError, /Invalid :connect option value/)
      end
    end

    %i(direct sharded replica_set load_balanced).each do |value|
      context "#{value}" do
        let(:string) { "mongodb://example.com/?connect=#{value}" }

        it_behaves_like 'parses successfully'

        it 'is a symbol' do
          expect(uri.uri_options[:connect]).to eq(value)
        end
      end
    end

    %i(replica-set load-balanced).each do |value|
      context "#{value}" do
        let(:string) { "mongodb://example.com/?connect=#{value}" }

        it_behaves_like 'parses successfully'

        it 'is a symbol' do
          expect(uri.uri_options[:connect]).to eq(value)
        end

        include_examples 'raises an error when client is created'
      end
    end

    context 'invalid value' do
      let(:string) { 'mongodb://example.com/?connect=bogus' }

      it_behaves_like 'parses successfully'

      it 'is a symbol' do
        expect(uri.uri_options[:connect]).to eq(:bogus)
      end

      include_examples 'raises an error when client is created'
    end
  end

  context 'connectTimeoutMS' do

    let(:uri_option) { 'connectTimeoutMS' }
    let(:ruby_option) { :connect_timeout }

    it_behaves_like 'a millisecond option'
  end

  context 'fsync' do

    let(:string) { 'mongodb://example.com/?fsync=true' }

    it_behaves_like 'parses successfully'

    it 'is a boolean' do
      expect(uri.uri_options[:write_concern]).to eq(BSON::Document.new(fsync: true))
    end
  end

  context 'heartbeatFrequencyMS' do

    let(:uri_option) { 'heartbeatFrequencyMS' }
    let(:ruby_option) { :heartbeat_frequency }

    it_behaves_like 'a millisecond option'
  end

  context 'journal' do

    let(:string) { 'mongodb://example.com/?journal=true' }

    it_behaves_like 'parses successfully'

    it 'is a boolean' do
      expect(uri.uri_options[:write_concern]).to eq(BSON::Document.new(j: true))
    end
  end

  context 'localThresholdMS' do

    let(:uri_option) { 'localThresholdMS' }
    let(:ruby_option) { :local_threshold }

    it_behaves_like 'a millisecond option'
  end

  context 'maxIdleTimeMS' do

    let(:uri_option) { 'maxIdleTimeMS' }
    let(:ruby_option) { :max_idle_time }

    it_behaves_like 'a millisecond option'
  end

  context 'maxStalenessSeconds' do

    let(:string) { "mongodb://example.com/?maxStalenessSeconds=123" }

    it_behaves_like 'parses successfully'

    it 'is an integer' do
      expect(uri.uri_options[:read][:max_staleness]).to be_a(Integer)
      expect(uri.uri_options[:read][:max_staleness]).to eq(123)
    end

    context '-1 as value' do
      let(:string) { "mongodb://example.com/?maxStalenessSeconds=-1" }

      it_behaves_like 'parses successfully'

      it 'is omitted' do
        uri.uri_options.should_not have_key(:read)
      end
    end
  end

  context 'maxPoolSize' do

    let(:uri_option) { 'maxPoolSize' }
    let(:ruby_option) { :max_pool_size }

    it_behaves_like 'an integer option'
  end

  context 'minPoolSize' do

    let(:uri_option) { 'minPoolSize' }
    let(:ruby_option) { :min_pool_size }

    it_behaves_like 'an integer option'
  end

  context 'readConcernLevel' do

    let(:string) { 'mongodb://example.com/?readConcernLevel=snapshot' }

    it_behaves_like 'parses successfully'

    it 'is a symbol' do
      expect(uri.uri_options[:read_concern]).to eq(BSON::Document.new(level: :snapshot))
    end
  end

  context 'readPreference' do

    let(:string) { "mongodb://example.com/?readPreference=nearest" }

    it_behaves_like 'parses successfully'

    it 'is a string' do
      expect(uri.uri_options[:read]).to eq(BSON::Document.new(mode: :nearest))
    end

    context 'an unknown value' do

      let(:string) { "mongodb://example.com/?readPreference=foobar" }

      it 'is unchanged' do
        expect(uri.uri_options[:read]).to eq(BSON::Document.new(mode: 'foobar'))
      end
    end
  end

  context 'readPreferenceTags' do

    let(:string) { "mongodb://example.com/?readPreferenceTags=dc:ny,rack:1" }

    it_behaves_like 'parses successfully'

    it 'parses correctly' do
      expect(uri.uri_options[:read]).to eq(BSON::Document.new(
        tag_sets: [{'dc' => 'ny', 'rack' => '1'}]))
    end

    context 'with double escaped keys and values' do

      let(:string) { "mongodb://example.com/?readPreferenceTags=dc%252f:ny,rack:1%252f" }

      it 'unescapes once' do
        expect(uri.uri_options[:read]).to eq(BSON::Document.new(
          tag_sets: [{'dc%2f' => 'ny', 'rack' => '1%2f'}]))
      end
    end
  end

  context 'replicaSet' do

    let(:uri_option) { 'replicaSet' }
    let(:ruby_option) { :replica_set }

    it_behaves_like 'a string option'
  end

  context 'retryWrites' do

    let(:uri_option) { 'retryWrites' }
    let(:ruby_option) { :retry_writes }

    it_behaves_like 'a boolean option'
  end

  context 'serverSelectionTimeoutMS' do

    let(:uri_option) { 'serverSelectionTimeoutMS' }
    let(:ruby_option) { :server_selection_timeout }

    it_behaves_like 'a millisecond option'
  end

  context 'socketTimeoutMS' do

    let(:uri_option) { 'socketTimeoutMS' }
    let(:ruby_option) { :socket_timeout }

    it_behaves_like 'a millisecond option'
  end

  context 'ssl' do

    let(:uri_option) { 'ssl' }
    let(:ruby_option) { :ssl }

    it_behaves_like 'a boolean option'
  end

  context 'tls' do

    let(:uri_option) { 'tls' }
    let(:ruby_option) { :ssl }

    it_behaves_like 'a boolean option'
  end

  context 'tlsAllowInvalidCertificates' do

    let(:uri_option) { 'tlsAllowInvalidCertificates' }
    let(:ruby_option) { :ssl_verify_certificate }

    it_behaves_like 'an inverted boolean option'
  end

  context 'tlsAllowInvalidHostnames' do

    let(:uri_option) { 'tlsAllowInvalidHostnames' }
    let(:ruby_option) { :ssl_verify_hostname }

    it_behaves_like 'an inverted boolean option'
  end

  context 'tlsCAFile' do

    let(:uri_option) { 'tlsCAFile' }
    let(:ruby_option) { :ssl_ca_cert }

    it_behaves_like 'a string option'
  end

  context 'tlsCertificateKeyFile' do

    let(:uri_option) { 'tlsCertificateKeyFile' }
    let(:ruby_option) { :ssl_cert }

    it_behaves_like 'a string option'
  end

  context 'tlsCertificateKeyFilePassword' do

    let(:uri_option) { 'tlsCertificateKeyFilePassword' }
    let(:ruby_option) { :ssl_key_pass_phrase }

    it_behaves_like 'a string option'
  end

  context 'tlsInsecure' do

    let(:uri_option) { 'tlsInsecure' }
    let(:ruby_option) { :ssl_verify }

    it_behaves_like 'an inverted boolean option'
  end

  context 'w' do

    context 'integer value' do
      let(:string) { "mongodb://example.com/?w=1" }

      it_behaves_like 'parses successfully'

      it 'is an integer' do
        expect(uri.uri_options[:write_concern]).to eq(BSON::Document.new(w: 1))
      end
    end

    context 'string value' do
      let(:string) { "mongodb://example.com/?w=foo" }

      it_behaves_like 'parses successfully'

      it 'is a string' do
        expect(uri.uri_options[:write_concern]).to eq(BSON::Document.new(w: 'foo'))
      end
    end

    context 'majority' do
      let(:string) { "mongodb://example.com/?w=majority" }

      it_behaves_like 'parses successfully'

      it 'is a symbol' do
        expect(uri.uri_options[:write_concern]).to eq(BSON::Document.new(w: :majority))
      end
    end
  end

  context 'waitQueueTimeoutMS' do

    let(:uri_option) { 'waitQueueTimeoutMS' }
    let(:ruby_option) { :wait_queue_timeout }

    it_behaves_like 'a millisecond option'
  end

  context 'wtimeoutMS' do

    let(:string) { "mongodb://example.com/?wtimeoutMS=100" }

    it_behaves_like 'parses successfully'

    it 'is a float' do
      expect(uri.uri_options[:write_concern]).to eq(BSON::Document.new(wtimeout: 100))
    end
  end

  context 'zlibCompressionLevel' do

    let(:uri_option) { 'zlibCompressionLevel' }
    let(:ruby_option) { :zlib_compression_level }

    let(:string) { "mongodb://example.com/?#{uri_option}=7" }

    it_behaves_like 'parses successfully'

    it 'is an integer' do
      expect(uri.uri_options[ruby_option]).to eq(7)
    end
  end
end
