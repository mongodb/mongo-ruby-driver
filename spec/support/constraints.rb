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
      if RUBY_PLATFORM =~ /\bjava\b/
        skip "MRI required, we have #{RUBY_PLATFORM}"
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
end
