module Constraints
  def min_server_version(version)
    unless version =~ /^\d+\.\d+$/
      raise ArgumentError, "Version can only be major.minor: #{version}"
    end

    before do
      if version > ClusterConfig.instance.server_version
        skip "Server version #{version} or higher required, we have #{ClusterConfig.instance.server_version}"
      end
    end
  end

  def max_server_version(version)
    unless version =~ /^\d+\.\d+$/
      raise ArgumentError, "Version can only be major.minor: #{version}"
    end

    before do
      if version < ClusterConfig.instance.short_server_version
        skip "Server version #{version} or lower required, we have #{ClusterConfig.instance.server_version}"
      end
    end
  end

  def min_server_fcv(version)
    unless version =~ /^\d+\.\d+$/
      raise ArgumentError, "FCV can only be major.minor: #{version}"
    end

    before do
      unless ClusterConfig.instance.fcv_ish >= version
        skip "FCV #{version} or higher required, we have #{ClusterConfig.instance.fcv_ish} (server #{ClusterConfig.instance.server_version})"
      end
    end
  end

  def max_server_fcv(version)
    unless version =~ /^\d+\.\d+$/
      raise ArgumentError, "Version can only be major.minor: #{version}"
    end

    before do
      if version < ClusterConfig.instance.fcv_ish
        skip "FCV #{version} or lower required, we have #{ClusterConfig.instance.fcv_ish} (server #{ClusterConfig.instance.server_version})"
      end
    end
  end

  def require_topology(*topologies)
    topologies = topologies.map { |t| t.to_s }
    invalid_topologies = topologies - %w(single replica_set sharded)
    unless invalid_topologies.empty?
      raise ArgumentError, "Invalid topologies requested: #{invalid_topologies.join(', ')}"
    end
    before do
      topology = authorized_client.cluster.topology.class.name.sub(/.*::/, '')
      topology = topology.gsub(/([A-Z])/) { |match| '_' + match.downcase }.sub(/^_/, '')
      if topology =~ /^replica_set/
        topology = 'replica_set'
      end
      unless topologies.include?(topology)
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
    min_server_fcv '4.0'
    require_topology :replica_set
  end

  def require_scram_sha_256_support
    before do
      $mongo_server_features ||= begin
        scanned_client_server!.features
      end
      unless $mongo_server_features.scram_sha_256_enabled?
        skip "SCRAM SHA 256 is not enabled on the server"
      end
    end
  end

  def require_ssl
    before do
      unless SpecConfig.instance.ssl?
        skip "SSL not enabled"
      end
    end
  end

  def require_local_tls
    before do
      unless SpecConfig.instance.ssl? && !SpecConfig.instance.ci?
        skip 'Not running locally with TLS enabled'
      end
    end
  end

  def require_no_retry_writes
    before do
      if SpecConfig.instance.retry_writes?
        skip "Retry writes is enabled"
      end
    end
  end

  def require_compression
    before do
      if SpecConfig.instance.compressors.nil?
        skip "Compression is not enabled"
      end
    end
  end

  def require_no_compression
    before do
      if SpecConfig.instance.compressors
        skip "Compression is enabled"
      end
    end
  end

  def min_ruby_version(version)
    before do
      if RUBY_VERSION < version
        skip "Ruby version #{version} or higher required"
      end
    end
  end

  def ruby_version_lt(version)
    before do
      if RUBY_VERSION >= version
        skip "Ruby version less than #{version} required"
      end
    end
  end

  def require_auth
    before do
      unless ENV['AUTH'] == 'auth' || ClusterConfig.instance.auth_enabled?
        skip "Auth required"
      end
    end
  end

  def require_no_auth
    before do
      if ENV['AUTH'] == 'auth' || ClusterConfig.instance.auth_enabled?
        skip "Auth not allowed"
      end
    end
  end
end
