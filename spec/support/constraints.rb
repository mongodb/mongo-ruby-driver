module Constraints
  def min_server_version(version)
    unless version =~ /^\d+\.\d+$/
      raise ArgumentError, "Version can only be major.minor: #{version}"
    end

    before do
      client = authorized_client
      $server_version ||= client.database.command(buildInfo: 1).first['version']

      if version > $server_version
        skip "Server version #{version} required, we have #{$server_version}"
      end
    end
  end

  def require_sessions
    before do
      unless sessions_enabled?
        skip 'Sessions are not enabled'
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
      unless topologies.include?(topology)
        skip "Topology #{topologies.join(' or ')} required, we have #{topology}"
      end
    end
  end

  # Constrain tests that use TimeoutInterrupt to MRI (and Unix)
  def only_mri
    before do
      if SpecConfig.instance.mri?
        skip "MRI required, we have #{SpecConfig.instance.platform}"
      end
    end
  end

  def max_example_run_time(timeout)
    around do |example|
      TimeoutInterrupt.timeout(timeout) do
        example.run
      end
    end
  end

  def require_transaction_support
    min_server_version '4.0'
    require_topology :replica_set
  end

  def require_scram_sha_256_support
    before do
      $mongo_server_features ||= begin
        $mongo_client ||= initialize_scanned_client!
        $mongo_client.cluster.servers.first.features
      end
      unless $mongo_server_features.scram_sha_256_enabled?
        skip "SCRAM SHA 256 is not enabled on the server"
      end
    end
  end
end
