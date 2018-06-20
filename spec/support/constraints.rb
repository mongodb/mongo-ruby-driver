module Constraints
  def min_server_version(version)
    unless version =~ /^\d+\.\d+$/
      raise ArgumentError, "Version can only be major.minor: #{version}"
    end

    client = $mongo_client ||= initialize_scanned_client!
    $server_version ||= client.database.command(buildInfo: 1).first['version']

    if version > $server_version
      before do
        skip "Server version #{version} required, we have #{$server_version}"
      end
    end
  end
end
