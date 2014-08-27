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
  begin
    mo = Mongo::Orchestration::Service.new
  rescue => ex
    raise "Mongo Orchestration service is not available, skipping all test that require orchestration"
    c.filter_run_excluding :orchestration => true
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

  it 'provides get method that sets object' do
    resource.get
    expect(resource.ok).to be true
    expect(resource.object).to be
  end
end

describe Mongo::Orchestration::Service, :orchestration => true do
  let(:service) { described_class.new }

  it 'initializes and checks service' do
    expect(service.response.parsed_response['service']).to eq('mongo-orchestration')
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

    mongodb_uri = cluster.object['mongodb_uri']
    expect(cluster.object['mongodb_uri']).to match(%r{:})

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
    expect(cluster.message_summary).to match(%r{GET /servers/standalone, options: {}, 404 Not Found})

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

  it 'provides member resources' do
    member_resources = cluster.member_resources
    expect(member_resources.size).to eq(3)
    member_resources.each do |member_resource|
      expect(member_resource).to be_instance_of(Mongo::Orchestration::Resource)
      expect(member_resource.base_path).to match(%r{/replica_sets/repl0/members/})
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
        [:secondaries, 2],
        [:arbiters,    0],
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

  it 'provides shard resources' do
    shard_resources = cluster.shard_resources
    expect(shard_resources.size).to eq(2)
    shard_resources.each do |member|
      expect(member).to be_instance_of(Mongo::Orchestration::Resource)
      expect(member.object['isServer']).to be true
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

  it 'provides configservers and routers' do
    [
        [:configservers, 1, %r{/servers/}],
        [:routers,       2, %r{/servers/}]
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
