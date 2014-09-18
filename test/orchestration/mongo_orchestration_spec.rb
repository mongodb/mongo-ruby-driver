# Copyright (C) 2009-2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# run rspec test via - rspec --format documentation --color -Itest/tools test/tools/mongo_orchestration_spec.rb

#require 'spec_helper'
require 'mongo_orchestration'

RSpec.configure do |c|
  c.filter_run_excluding :broken => true
  begin
    mo = Mongo::Orchestration::Service.new
  rescue => ex
    c.filter_run_excluding :orchestration => true
    raise "Mongo Orchestration service is not available, skipping all test that require orchestration"
  end
end

describe Mongo::Orchestration::Base, :orchestration => true do
  let(:base) { described_class.new }

  it 'provides http_request method' do
    base.http_request(:get)
    expect(base.response.code).to eq(200)
    expect(base.response.parsed_response['service']).to eq('mongo-orchestration')
  end

  it 'provides get method and checks ok' do
    base.get
    expect(base.response.code).to eq(200)
    expect(base.response.parsed_response['service']).to eq('mongo-orchestration')
    expect(base.response.response.class.name).to eq("Net::HTTPOK")
    expect(base.humanized_http_response_class_name).to eq("OK")
    expect(base.message_summary).to match(/^GET .* OK,.* JSON:/)
    expect(base.ok).to be true
  end
end

describe Mongo::Orchestration::Resource, :orchestration => true do
  let(:resource) { described_class.new }

  it 'provides initialize that sets object' do
    expect(resource.ok).to be true
    expect(resource.object).to be
  end
end

describe Mongo::Orchestration::Service, :orchestration => true do
  let(:service) { described_class.new }

  it 'initializes and checks service' do
    expect(service.response.parsed_response['service']).to eq('mongo-orchestration')
    expect(service.response.parsed_response['version']).to eq('0.9')
  end
end

standalone_config = {
    orchestration: "servers",
    request_content: {
        id: "standalone",
        name: "mongod",
        procParams: {
            journal: true
        }
    }
}

describe Mongo::Orchestration::Cluster, :orchestration => true do
  let(:service) { Mongo::Orchestration::Service.new }
  let(:cluster) { service.configure(standalone_config) }

  it 'runs init, status, stop, start, restart and destroy methods' do
    cluster.destroy # force destroyed

    cluster.init
    expect(cluster.message_summary).to match(%r{^PUT /servers/standalone, options: {.*}, 200 OK, response JSON:})
    expect(cluster.object).to be
    expect(cluster.object['serverInfo']['ok']).to eq(1.0)

    cluster.init # init for already init'ed
    expect(cluster.message_summary).to match(%r{^GET /servers/standalone, options: {}, 200 OK, response JSON:})

    cluster.status # status for init'ed
    expect(cluster.message_summary).to match(%r{^GET /servers/standalone, options: {}, 200 OK, response JSON:})

    expect(cluster.object['mongodb_uri']).to match(%r{:})
    mongodb_uri = cluster.object['mongodb_uri']
    # add client connection when Ruby is ready for prime time

    cluster.stop
    expect(cluster.message_summary).to match(%r{^POST /servers/standalone, options: {:body=>\"{\\\"action\\\":\\\"stop\\\"}\"}, 200 OK})

    cluster.start
    expect(cluster.message_summary).to match(%r{^POST /servers/standalone, options: {:body=>\"{\\\"action\\\":\\\"start\\\"}\"}, 200 OK})

    cluster.start # start already started, check external pgrep mongo
    expect(cluster.message_summary).to match(%r{^POST /servers/standalone, options: {:body=>\"{\\\"action\\\":\\\"start\\\"}\"}, 200 OK})

    cluster.restart
    expect(cluster.message_summary).to match(%r{^POST /servers/standalone, options: {:body=>\"{\\\"action\\\":\\\"restart\\\"}\"}, 200 OK})

    cluster.destroy
    expect(cluster.message_summary).to match(%r{^DELETE /servers/standalone, options: {}, 204 No Content})

    cluster.destroy # destroy for already destroyed
    expect(cluster.message_summary).to match(%r{DELETE /servers/standalone, options: {}, 404 Not Found})

    cluster.status # status for destroyed
    expect(cluster.message_summary).to match(%r{GET /servers/standalone, options: {}, 404 Not Found})
  end
end

describe Mongo::Orchestration::Server, :orchestration => true do
  let(:cluster) { @cluster }

  before(:all) do
    @service = Mongo::Orchestration::Service.new
    @cluster = @service.configure(standalone_config)
  end

  after(:all) do
    @cluster.destroy
  end

  it 'configures a cluster/server' do
    expect(cluster).to be_kind_of(Mongo::Orchestration::Cluster)
    expect(cluster).to be_instance_of(Mongo::Orchestration::Server)
    expect(cluster.object['orchestration']).to eq('servers')
    expect(cluster.object['mongodb_uri']).to match(%r{:})
    expect(cluster.object['procInfo']).to be
  end

  it 'configures the same cluster/server and does not configure a duplicate' do
    same_server = @service.configure(standalone_config)
    expect(same_server.object['mongodb_uri']).to eq(cluster.object['mongodb_uri'])
    same_server.destroy
  end
end

replicaset_config = {
    orchestration: "replica_sets",
    request_content: {
        id: "repl0",
        members: [
            {
                procParams: {
                    nohttpinterface: true,
                    journal: true,
                    noprealloc: true,
                    nssize: 1,
                    oplogSize: 150,
                    smallfiles: true
                },
                rsParams: {
                    priority: 99
                }
            },
            {
                procParams: {
                    nohttpinterface: true,
                    journal: true,
                    noprealloc: true,
                    nssize: 1,
                    oplogSize: 150,
                    smallfiles: true
                },
                rsParams: {
                    priority: 1.1
                }
            },
            {
                procParams: {
                    nohttpinterface: true,
                    journal: true,
                    noprealloc: true,
                    nssize: 1,
                    oplogSize: 150,
                    smallfiles: true
                },
                rsParams: {
                    arbiterOnly: true
                }
            }
        ]
    }
}

describe Mongo::Orchestration::ReplicaSet, :orchestration => true do
  let(:cluster) { @cluster }

  before(:all) do
    @service = Mongo::Orchestration::Service.new
    @cluster = @service.configure(replicaset_config)
  end

  after(:all) do
    @cluster.destroy
  end

  describe 'not yet implemented', :broken => true do
    it 'provides member resources' do
      member_resources = cluster.member_resources
      expect(member_resources.size).to eq(3)
      member_resources.each do |member_resource|
        expect(member_resource).to be_instance_of(Mongo::Orchestration::Resource)
        expect(member_resource.base_path).to match(%r{/replica_sets/repl0/members/})
        expect(member_resource.object['uri']).to match(%r{^/}) # uri abs_path is not completed
      end
    end
  end

  it 'provides primary' do
    server = cluster.primary
    expect(server).to be_instance_of(Mongo::Orchestration::Server) # check object mongodb_uri
    expect(server.base_path).to match(%r{/servers/})
    expect(server.object['orchestration']).to eq('servers')
    expect(server.object['mongodb_uri']).to match(%r{:})
    expect(server.object['procInfo']).to be
  end

  it 'provides members, secondaries, arbiters and hidden member methods' do
    [
        [:members,     3],
        [:secondaries, 1],
        [:arbiters,    1],
        [:hidden,      0]
    ].each do |method, size|
      servers = cluster.send(method)
      expect(servers.size).to eq(size)
      servers.each do |server|
        expect(server).to be_instance_of(Mongo::Orchestration::Server)
        expect(server.base_path).to match(%r{/servers/})
        expect(server.object['orchestration']).to eq('servers')
        expect(server.object['mongodb_uri']).to match(%r{:})
        expect(server.object['procInfo']).to be
      end
    end
  end

  it 'gracefully handles requests when there is no primary' do
    primary = cluster.primary
    secondaries = cluster.secondaries
    arbiters = cluster.arbiters
    arbiters.first.stop
    primary.stop
    server = Mongo::MongoClient.from_uri(secondaries.first.object['mongodb_uri'])
    db = server['test']
    p db.command({isMaster: 1})
  end

  # it 'gracefully handles requests when there are no secondaries' do
  #   cluster.secondaries.first.stop
  # end
end

sharded_configuration = {
    orchestration: "sharded_clusters",
    request_content: {
        id: "shard_cluster_1",
        configsvrs: [
            {
            }
        ],
        shards: [
            {
                id: "sh1",
                shardParams: {
                    procParams: {
                    }
                }
            },
            {
                id: "sh2",
                shardParams: {
                    procParams: {
                    }
                }
            }
        ],
        routers: [
            {
            },
            {
            }
        ]
    }
}

describe Mongo::Orchestration::ShardedCluster, :orchestration => true do
  let(:cluster) { @cluster }

  before(:all) do
    @service = Mongo::Orchestration::Service.new
    @cluster = @service.configure(sharded_configuration)
  end

  after(:all) do
    @cluster.destroy
  end

  describe 'not yet implemented', :broken => true do
    it 'provides shard resources' do
      shard_resources = cluster.shard_resources
      expect(shard_resources.size).to eq(2)
      shard_resources.each do |shard_resource|
        expect(shard_resource).to be_instance_of(Mongo::Orchestration::Resource)
        expect(shard_resource.base_path).to match(%r{^/sharded_clusters/shard_cluster_1/shards/})
        expect(shard_resource.object['isServer']).to be
        expect(shard_resource.object['uri']).to match(%r{^/}) # uri abs_path is not completed
      end
    end
  end

  it 'provides single-server shards' do
    shards = cluster.shards
    expect(shards.size).to eq(2)
    shards.each do |shard|
      expect(shard).to be_instance_of(Mongo::Orchestration::Server)
      expect(shard.base_path).to match(%r{/servers/})
      expect(shard.object['orchestration']).to eq('servers')
      expect(shard.object['mongodb_uri']).to match(%r{:})
      expect(shard.object['procInfo']).to be
    end
  end

  it 'provides configsvrs and routers' do
    [
        [:configsvrs, 1, %r{/servers/}],
        [:routers,    2, %r{/servers/}]
    ].each do |method, size, base_path|
      servers = cluster.send(method)
      expect(servers.size).to eq(size)
      servers.each do |server|
        expect(server).to be_instance_of(Mongo::Orchestration::Server)
        expect(server.base_path).to match(%r{/servers/})
        expect(server.object['orchestration']).to eq('servers')
        expect(server.object['mongodb_uri']).to match(%r{:})
        expect(server.object['procInfo']).to be
      end
    end
  end
end


sharded_rs_configuration = {
    orchestration: "sharded_clusters",
    request_content: {
        id: "shard_cluster_2",
        configsvrs: [
            {
            }
        ],
        shards: [
            {
                id: "sh1",
                shardParams: {
                    members: [{},{},{}]
                }
            },
            {
                id: "sh2",
                shardParams: {
                    members: [{},{},{}]
                }
            }
        ],
        routers: [
            {
            },
            {
            }
        ]
    }
}

describe Mongo::Orchestration::ShardedCluster, :orchestration => true do
  let(:cluster) { @cluster }

  before(:all) do
    @service = Mongo::Orchestration::Service.new
    @cluster = @service.configure(sharded_rs_configuration)
  end

  after(:all) do
    @cluster.destroy
  end

  describe 'not yet implemented', :broken => true do
    it 'provides shard resources' do
      shard_resources = cluster.shard_resources
      expect(shard_resources.size).to eq(2)
      shard_resources.each do |shard_resource|
        expect(shard_resource).to be_instance_of(Mongo::Orchestration::Resource)
        expect(shard_resource.base_path).to match(%r{^/sharded_clusters/shard_cluster_2/shards/})
        expect(shard_resource.object['isReplicaSet']).to be
        expect(shard_resource.object['uri']).to match(%r{^/}) # uri abs_path is not completed
      end
    end
  end

  it 'provides replica-set shards' do
    shards = cluster.shards
    expect(shards.size).to eq(2)
    shards.each do |shard|
      expect(shard).to be_instance_of(Mongo::Orchestration::ReplicaSet)
      expect(shard.base_path).to match(%r{/replica_sets/})
      expect(shard.object['orchestration']).to eq('replica_sets')
      expect(shard.object['mongodb_uri']).to match(%r{:})
      expect(shard.object['members']).to be
    end
  end
end

hosts_preset_config = {
    orchestration: 'servers',
    request_content: {
        id: 'host_preset_1',
        preset: 'basic.json'
    }
}

rs_preset_config = {
    orchestration: 'replica_sets',
    request_content: {
        id: 'rs_preset_1',
        preset: 'basic.json'
    }
}

sh_preset_config = {
    orchestration: 'sharded_clusters',
    request_content: {
        id: 'sh_preset_1',
        preset: 'basic.json'
    }
}

describe 'Service configure preset Cluster', :orchestration => true do
  let(:service) { @service }
  let(:preset_configs) { [ hosts_preset_config, rs_preset_config, sh_preset_config ] }

  before(:all) do
    @service = Mongo::Orchestration::Service.new
  end

  it 'configures presets with id' do
    preset_configs.each do |preset_config|
      cluster = service.configure(preset_config)
      expect(cluster.object['orchestration']).to eq(preset_config[:orchestration])
      expect(cluster.object['id']).to eq(preset_config[:request_content][:id])
      cluster.destroy
    end
  end

  it 'configures presets with id deleted' do
    preset_configs.each do |preset_config|
      preset_config[:request_content].delete(:id)
      cluster = service.configure(preset_config)
      expect(cluster.object['orchestration']).to eq(preset_config[:orchestration])
      cluster.destroy
    end
  end
end

