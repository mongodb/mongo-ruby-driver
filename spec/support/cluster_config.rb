require 'singleton'

class ClusterConfig
  include Singleton

  def scanned_client
    $mongo_client ||= initialize_scanned_client!
  end

  def single_server?
    scanned_client.cluster.servers.length == 1
  end

  def server!
    server = scanned_client.cluster.servers.first
    if server.nil?
      raise ScannedClientHasNoServers
    end
    server
  end

  def mongos?
    server!.mongos?
  end

  def replica_set_name
    server!.replica_set_name
  end
end
