# frozen_string_literal: true
# encoding: utf-8

module Mrss
  module Constraints
    def min_server_version(version)
      parsed_version = Gem::Version.new(version)

      before(:all) do
        if parsed_version > Gem::Version.new(ClusterConfig.instance.server_version)
          skip "Server version #{version} or higher required, we have #{ClusterConfig.instance.server_version}"
        end
      end
    end

    def max_server_version(version)
      parsed_version = Gem::Version.new(version)

      before(:all) do
        if parsed_version < Gem::Version.new(ClusterConfig.instance.server_version)
          skip "Server version #{version} or lower required, we have #{ClusterConfig.instance.server_version}"
        end
      end
    end

    def min_server_fcv(version)
      parsed_version = Gem::Version.new(version)

      before(:all) do
        unless Gem::Version.new(ClusterConfig.instance.fcv_ish) >= parsed_version
          skip "FCV #{version} or higher required, we have #{ClusterConfig.instance.fcv_ish} (server #{ClusterConfig.instance.server_version})"
        end
      end
    end

    def max_server_fcv(version)
      parsed_version = Gem::Version.new(version)

      before(:all) do
        if parsed_version < Gem::Version.new(ClusterConfig.instance.fcv_ish)
          skip "FCV #{version} or lower required, we have #{ClusterConfig.instance.fcv_ish} (server #{ClusterConfig.instance.server_version})"
        end
      end
    end

    def require_topology(*topologies)
      invalid_topologies = topologies - [:single, :replica_set, :sharded, :load_balanced]

      unless invalid_topologies.empty?
        raise ArgumentError, "Invalid topologies requested: #{invalid_topologies.join(', ')}"
      end

      before(:all) do
        unless topologies.include?(topology = ClusterConfig.instance.topology)
          skip "Topology #{topologies.join(' or ')} required, we have #{topology}"
        end
      end
    end

    def max_example_run_time(timeout)
      around do |example|
        TimeoutInterrupt.timeout(timeout, TimeoutInterrupt::Error.new("Test execution terminated after #{timeout} seconds")) do
          example.run
        end
      end
    end

    def require_transaction_support
      before(:all) do
        case ClusterConfig.instance.topology
        when :single
          skip 'Transactions tests require a replica set (4.0+) or a sharded cluster (4.2+)'
        when :replica_set
          unless ClusterConfig.instance.server_version >= '4.0'
            skip 'Transactions tests in a replica set topology require server 4.0+'
          end
        when :sharded, :load_balanced
          unless ClusterConfig.instance.server_version >= '4.2'
            skip 'Transactions tests in a sharded cluster topology require server 4.2+'
          end
        else
          raise NotImplementedError
        end
      end
    end

    # Fail command fail point was added to mongod in 4.0 and to mongos in 4.2.
    def require_fail_command
      require_transaction_support
    end

    def require_tls
      before(:all) do
        unless SpecConfig.instance.ssl?
          skip "SSL not enabled"
        end
      end
    end

    def require_no_tls
      before(:all) do
        if SpecConfig.instance.ssl?
          skip "SSL enabled"
        end
      end
    end

    def require_retry_writes
      before(:all) do
        unless SpecConfig.instance.retry_writes?
          skip "Retry writes is disabled"
        end
      end
    end

    def require_no_retry_writes
      before(:all) do
        if SpecConfig.instance.retry_writes?
          skip "Retry writes is enabled"
        end
      end
    end

    def require_compression
      before(:all) do
        if SpecConfig.instance.compressors.nil?
          skip "Compression is not enabled"
        end
      end
    end

    def require_zlib_compression
      before(:all) do
        compressors = SpecConfig.instance.compressors
        unless compressors && compressors.include?('zlib')
          skip "Zlib compression is not enabled"
        end
      end
    end

    def require_snappy_compression
      before(:all) do
        compressors = SpecConfig.instance.compressors
        unless compressors && compressors.include?('snappy')
          skip "Snappy compression is not enabled"
        end
      end
    end

    def require_no_snappy_compression
      before(:all) do
        compressors = SpecConfig.instance.compressors
        if compressors && compressors.include?('snappy')
          skip "Snappy compression is enabled"
        end
      end
    end

    def require_zstd_compression
      before(:all) do
        compressors = SpecConfig.instance.compressors
        unless compressors && compressors.include?('zstd')
          skip "Zstd compression is not enabled"
        end
      end
    end

    def require_no_zstd_compression
      before(:all) do
        compressors = SpecConfig.instance.compressors
        if compressors && compressors.include?('zstd')
          skip "Zstd compression is enabled"
        end
      end
    end

    def require_no_compression
      before(:all) do
        if SpecConfig.instance.compressors
          skip "Compression is enabled"
        end
      end
    end

    def ruby_version_gte(version)
      before(:all) do
        if RUBY_VERSION < version
          skip "Ruby version #{version} or higher required"
        end
      end
    end

    def ruby_version_lt(version)
      before(:all) do
        if RUBY_VERSION >= version
          skip "Ruby version less than #{version} required"
        end
      end
    end

    def require_auth(*values)
      before(:all) do
        if values.any?
          unless values.include?(ENV['AUTH'])
            msg = values.map { |v| "AUTH=#{v}" }.join(' or ')
            skip "This test requires #{msg}"
          end
        else
          unless ENV['AUTH'] == 'auth' || SpecConfig.instance.user || ClusterConfig.instance.auth_enabled?
            skip "Auth required"
          end
        end
      end
    end

    def require_no_auth
      before(:all) do
        auth = ENV.fetch('AUTH', '')
        if (!auth.empty? && auth != 'noauth') || SpecConfig.instance.user || ClusterConfig.instance.auth_enabled?
          skip "Auth not allowed"
        end
      end
    end

    def require_x509_auth
      before(:all) do
        unless SpecConfig.instance.x509_auth?
          skip "X.509 auth required"
        end
      end
    end

    def require_no_external_user
      before(:all) do
        if SpecConfig.instance.external_user?
          skip "External user configurations are not compatible with this test"
        end
      end
    end

    # Can the driver specify a write concern that won't be overridden?
    # (mongos 4.0+ overrides the write concern)
    def require_set_write_concern
      before(:all) do
        if %i(sharded load_balanced).include?(ClusterConfig.instance.topology) &&
          ClusterConfig.instance.short_server_version >= '4.0'
        then
          skip "mongos 4.0+ overrides write concern"
        end
      end
    end

    def require_multi_mongos
      before(:all) do
        if ClusterConfig.instance.topology == :sharded && SpecConfig.instance.addresses.length == 1
          skip 'Test requires a minimum of two mongoses if run in sharded topology'
        end

        if ClusterConfig.instance.topology == :load_balanced && SpecConfig.instance.single_mongos?
          skip 'Test requires a minimum of two mongoses if run in load-balanced topology'
        end
      end
    end

    # In sharded topology operations are distributed to the mongoses.
    # When we set fail points, the fail point may be set on one mongos and
    # operation may be executed on another mongos, causing failures.
    # Tests that are not setting targeted fail points should utilize this
    # method to restrict themselves to single mongos.
    #
    # In load-balanced topology, the same problem can happen when there is
    # more than one mongos behind the load balancer.
    def require_no_multi_mongos
      before(:all) do
        if ClusterConfig.instance.topology == :sharded && SpecConfig.instance.addresses.length > 1
          skip 'Test requires a single mongos if run in sharded topology'
        end
        if ClusterConfig.instance.topology == :load_balanced && !SpecConfig.instance.single_mongos?
          skip 'Test requires a single mongos, as indicated by SINGLE_MONGOS=1 environment variable, if run in load-balanced topology'
        end
      end
    end

    alias :require_no_multi_shard :require_no_multi_mongos

    def require_wired_tiger
      before(:all) do
        # Storage detection fails for serverless instances. However, it is safe to
        # assume that a serverless instance uses WiredTiger Storage Engine.
        if !SpecConfig.instance.serverless? && ClusterConfig.instance.storage_engine != :wired_tiger
          skip 'Test requires WiredTiger storage engine'
        end
      end
    end

    def require_wired_tiger_on_36
      before(:all) do
        if ClusterConfig.instance.short_server_version >= '3.6'
          # Storage detection fails for serverless instances. However, it is safe to
          # assume that a serverless instance uses WiredTiger Storage Engine.
          if !SpecConfig.instance.serverless? && ClusterConfig.instance.storage_engine != :wired_tiger
            skip 'Test requires WiredTiger storage engine on 3.6+ servers'
          end
        end
      end
    end

    def require_mmapv1
      before(:all) do
        if SpecConfig.instance.serverless? || ClusterConfig.instance.storage_engine != :mmapv1
          skip 'Test requires MMAPv1 storage engine'
        end
      end
    end

    def require_enterprise
      before(:all) do
        unless ClusterConfig.instance.enterprise?
          skip 'Test requires enterprise build of MongoDB'
        end
      end
    end

    # Integration tests for SRV polling require internet connectivity to
    # look up SRV records and a sharded cluster configured on default port on
    # localhost (localhost:27017, localhost:27018).
    def require_default_port_deployment
      # Because the DNS records at test1.test.build.10gen.cc point to
      # localhost:27017 & localhost:27018, the test suite must have been
      # configured to use these addresses
      before(:all) do
        have_default_port = SpecConfig.instance.addresses.any? do |address|
          %w(127.0.0.1 127.0.0.1:27017 localhost localhost:27017).include?(address)
        end
        unless have_default_port
          skip 'This test requires the test suite to be configured for localhost:27017'
        end
      end
    end

    # Some tests perform assertions on what the driver is logging.
    # Some test configurations, for example OCSP with unknown response,
    # produce warnings due to optional checks failing.
    # This constraint skips tests that issue logging assertions on configurations
    # that may produce non-test-originated log entries.
    def require_warning_clean
      before(:all) do
        if ENV['OCSP_STATUS'] == 'unknown'
          skip 'Unknown OCSP status is not global warning-clean'
        end
      end
    end

    def require_required_api_version
      before(:all) do
        unless ENV['API_VERSION_REQUIRED'] == '1'
          skip 'Set API_VERSION_REQUIRED=1 to run this test'
        end
      end
    end

    def require_no_required_api_version
      before(:all) do
        if ENV['API_VERSION_REQUIRED'] == '1'
          skip 'Cannot have API_VERSION_REQUIRED=1 to run this test'
        end
      end
    end

    def require_unix_socket
      before(:all) do
        if ENV['TOPOLOGY'] == 'load-balanced'
          skip 'Load balancer does not listen on Unix sockets'
        end
      end
    end
  end
end
