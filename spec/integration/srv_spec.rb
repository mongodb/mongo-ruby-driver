# frozen_string_literal: true
# rubocop:todo all

require 'spec_helper'

describe 'SRV lookup' do
  context 'end to end' do
    require_external_connectivity

    # JRuby apparently does not implement non-blocking UDP I/O which is used
    # by RubyDNS:
    # NotImplementedError: recvmsg_nonblock is not implemented
    fails_on_jruby

    before(:all) do
      require 'support/dns'
    end

    let(:uri) do
      "mongodb+srv://test-fake.test.build.10gen.cc/?tls=#{SpecConfig.instance.ssl?}&tlsInsecure=true"
    end

    let(:client) do
      new_local_client(uri,
        SpecConfig.instance.ssl_options.merge(
          server_selection_timeout: 3.16,
          timeout: 4.11,
          connect_timeout: 4.12,
          resolv_options: {
            nameserver: 'localhost',
            nameserver_port: [['localhost', 5300], ['127.0.0.1', 5300]],
          },
        ),
      )
    end

    context 'DNS resolver not responding' do
      it 'fails to create client' do
        lambda do
          client
        end.should raise_error(Mongo::Error::NoSRVRecords, /The DNS query returned no SRV records for 'test-fake.test.build.10gen.cc'/)
      end

      it 'times out in connect_timeout' do
        start_time = Mongo::Utils.monotonic_time

        lambda do
          client
        end.should raise_error(Mongo::Error::NoSRVRecords)

        elapsed_time = Mongo::Utils.monotonic_time - start_time
        elapsed_time.should > 4
        # The number of queries performed depends on local DNS search suffixes,
        # therefore we cannot reliably assert how long it would take for this
        # resolution to time out.
        #elapsed_time.should < 8
      end
    end
  end
end
