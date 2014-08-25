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
    c.filter_run_excluding :orchestration => true
  end
end

mo = Mongo::Orchestration::Service.new

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
    orchestration: "hosts",
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
    expect(cluster.message_summary).to match(%r{^PUT /hosts/standalone, options: {.*}, 200 OK, response JSON:})
    expect(cluster.object).to be
    expect(cluster.object['serverInfo']['ok']).to eq(1.0)

    cluster.init # init for already inited
    expect(cluster.message_summary).to match(%r{^GET /hosts/standalone, options: {}, 200 OK, response JSON:})

    cluster.status # status for inited
    expect(cluster.message_summary).to match(%r{^GET /hosts/standalone, options: {}, 200 OK, response JSON:})

    uri = cluster.object['uri']
    expect(cluster.object['uri']).to match(%r{:})

    # add client connection when Ruby is ready for prime time

    cluster.stop
    expect(cluster.message_summary).to match(%r{^POST /hosts/standalone, options: {:body=>\"{\\\"action\\\":\\\"stop\\\"}\"}, 200 OK})

    cluster.start
    expect(cluster.message_summary).to match(%r{^POST /hosts/standalone, options: {:body=>\"{\\\"action\\\":\\\"start\\\"}\"}, 200 OK})

    cluster.restart
    expect(cluster.message_summary).to match(%r{^POST /hosts/standalone, options: {:body=>\"{\\\"action\\\":\\\"restart\\\"}\"}, 200 OK})

    cluster.destroy
    expect(cluster.message_summary).to match(%r{^DELETE /hosts/standalone, options: {}, 204 No Content})

    cluster.destroy # destroy for already destroyed
    expect(cluster.message_summary).to match(%r{GET /hosts/standalone, options: {}, 404 Not Found})

    cluster.status # status for destroyed
    expect(cluster.message_summary).to match(%r{GET /hosts/standalone, options: {}, 404 Not Found})
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

  it 'configures a cluster/host' do
    expect(cluster).to be_kind_of(Mongo::Orchestration::Cluster)
    expect(cluster).to be_instance_of(Mongo::Orchestration::Server)
    expect(cluster.object['orchestration']).to eq('hosts')
    expect(cluster.object['uri']).to match(%r{:})
    expect(cluster.object['procInfo']).to be
  end

  it 'configures the same cluster/host and does not configure a duplicate' do
    same_server = @service.configure(standalone_config)
    expect(same_server.object['uri']).to eq(cluster.object['uri'])
    same_server.destroy
  end
end

replicaset_config = {
    orchestration: "rs",
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

  it 'provides primary' do
    server = @cluster.primary
    expect(server).to be_instance_of(Mongo::Orchestration::Server) # check object uri
    expect(server.base_path).to match(%r{/hosts/})
    expect(server.object['orchestration']).to eq('hosts')
    expect(server.object['uri']).to match(%r{:})
    expect(server.object['procInfo']).to be
  end

  it 'provides secondaries, arbiters and hidden member methods' do
    [
        [:members,     3, %r{/hosts/}],
        [:secondaries, 2, %r{/hosts/}],
        [:arbiters,    0, %r{/hosts/}],
        [:hidden,      0, %r{/hosts/}]
    ].each do |method, size, base_path|
      servers = @cluster.send(method)
      expect(servers.size).to eq(size)
      servers.each do |server|
        expect(server).to be_instance_of(Mongo::Orchestration::Server)
        expect(server.base_path).to match(base_path)
        expect(server.object['orchestration']).to eq('hosts')
        expect(server.object['uri']).to match(%r{:})
        expect(server.object['procInfo']).to be
      end
    end
  end
end

sharded_configuration = {
    orchestration: "sh",
    request_content: {
        id: "shard_cluster_1",
        configsvrs: [
            {
            }
        ],
        members: [
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

  it 'provides single-server shards' do
    shards = @cluster.shards
    expect(shards.size).to eq(2)
    shards.each do |shard|
      expect(shard).to be_instance_of(Mongo::Orchestration::Server)
      expect(shard.base_path).to match(%r{/hosts/})
      expect(shard.object['orchestration']).to eq('hosts')
      expect(shard.object['uri']).to match(%r{:})
      expect(shard.object['procInfo']).to be
    end
  end

  it 'provides members' do
    members = @cluster.members
    expect(members.size).to eq(2)
    members.each do |member|
      expect(member).to be_instance_of(Mongo::Orchestration::Resource)
      expect(member.object['isHost']).to be true
    end
  end

  it 'provides configservers and routers' do
    [
        [:configservers, 1, %r{/hosts/}],
        [:routers,       2, %r{/hosts/}]
    ].each do |method, size, base_path|
      servers = @cluster.send(method)
      expect(servers.size).to eq(size)
      servers.each do |server|
        expect(server).to be_instance_of(Mongo::Orchestration::Server)
        expect(server.base_path).to match(%r{/hosts/})
        expect(server.object['orchestration']).to eq('hosts')
        expect(server.object['uri']).to match(%r{:})
        expect(server.object['procInfo']).to be
      end
    end
  end
end


sharded_rs_configuration = {
    orchestration: "sh",
    request_content: {
        id: "shard_cluster_2",
        configsvrs: [
            {
            }
        ],
        members: [
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

  it 'provides rs shards' do
    shards = @cluster.shards
    expect(shards.size).to eq(2)
    shards.each do |shard|
      expect(shard).to be_instance_of(Mongo::Orchestration::ReplicaSet)
      expect(shard.base_path).to match(%r{/rs/})
      expect(shard.object['orchestration']).to eq('rs')
      expect(shard.object['uri']).to match(%r{:})
      expect(shard.object['members']).to be
    end
  end
end
