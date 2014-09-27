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

$ORCH_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', 'test', 'orchestration'))
$LOAD_PATH.unshift($ORCH_DIR)
$LIB_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', 'lib'))
$LOAD_PATH.unshift($LIB_DIR)

require 'bson'
require 'mongo'
require 'mongo_orchestration'
require 'rspec'

# eliminate ping commands from opcounters
module Mongo
  class Pool
    def refresh_ping_time
      rand
    end
  end
end

TEST_DB = 'test'
TEST_COLL = 'test'
TEST_COLL_OUT = 'test_out'
STATUS_THRESHOLD = {
    'opcounters' => {
        'query' => 0,
        'getmore' => 0,
        'command' => 1},
    'cursors' => {
        'totalOpen' => 0}
}
COLL_COMMANDS = %w[
    aggregate
    collStats
    count
    delete
    findAndModify
    insert
    mapReduce
    parallelCollectionScan
    reIndex
    update
]

TEST_DOCS = [
    {coordinates: [-74.044491, 40.689522],  name: 'The Statue of Liberty National Monument', city: 'New York', state: 'NY'},
    {coordinates: [-71.056454, 42.360059],  name: 'Freedom Trail', city: 'Boston', state: 'MA'},
    {coordinates: [-75.149877, 39.949030],  name: 'Independence Hall', city: 'Philadelphia', state: 'PA'},
    {coordinates: [-79.874562, 32.754481],  name: 'Fort Sumter National Monument', city: 'Charleston', state: 'SC'},
    {coordinates: [-77.022959, 38.890101],  name: 'The National Mall', city: 'Washington', state: 'DC'},
    {coordinates: [-103.459066, 43.879317], name: 'Mt Rushmore National Memorial', city: 'Keystone', state: 'SD'},
    {coordinates: [-98.486094, 29.426215],  name: 'The Alamo', city: 'San Antonio', state: 'TX'},
    {coordinates: [-90.184726, 38.624889],  name: 'The Gateway Arch', city: 'St Louis', state: 'MO'},
    {coordinates: [-114.737727, 36.016287], name: 'Hoover Dam', location: 'Clark County, NV / Mohave County, AZ'},
    {coordinates: [-77.222400, 39.811603],  name: 'Gettysburg National Military Park', city: 'Gettysburg', state: 'PA'},
    {coordinates: [-77.050177, 38.889491],  name: 'Lincoln Memorial', city: 'Washington', state: 'DC'},
    {coordinates: [-122.422960, 37.830588], name: 'Alcatraz Island', city: 'San Franciso', state: 'CA'},
    {coordinates: [-157.949973, 21.364897], name: 'USS Arizona Memorial', city: 'Honolulu', state: 'HI'},
    {coordinates: [-87.623277, 41.882917],  name: 'Cloud Gate', city: 'Chicago', state: 'IL'},
    {coordinates: [-77.047613, 38.891261],  name: 'Vietnam Veterans Memorial', city: 'Washington', state: 'DC'},
    {coordinates: [-73.982237, 40.753406],  name: 'New York Public Library', city: 'New York', state: 'NY'},
    {coordinates: [-81.091239, 32.073624],  name: 'The Cathedral of Saint John the Baptist', city: 'Savannah', state: 'GA'},
    {coordinates: [-77.047762, 38.888071],  name: 'Korean War Veterans Memorial', city: 'Washington', state: 'DC'},
    {coordinates: [-73.986209, 40.756819],  name: 'Times Square', city: 'New York', state: 'NY'},
    {coordinates: [-74.013462, 40.713183],  name: 'One World Trade Center', city: 'New York', state: 'NY'},
    {coordinates: [-77.009054, 38.890042],  name: 'United States Capitol', city: 'Washington', state: 'DC'}
]

$topologies ||= Hash.new

at_exit do
  $topologies.values.each {|topology| topology.delete } unless ENV['TOPOLOGY_NO_DESTROY']
end

public

def find_one_ordinal(opts = {})
  @coll.find_one({"a" => @ordinal}, opts)
end

def rescue_connection_failure_and_retry(max_retries = 30)
  retries = 0
  begin
    yield
  rescue Mongo::ConnectionFailure => ex
    retries += 1
    raise ex if retries > max_retries
    sleep(2)
    retry
  end
end

def with_rescue(exception_class = Exception)
  begin
    yield
  rescue exception_class => ex
    ex
  end
end

def result_response
  @response = nil
  @result = begin
    @response = yield
  rescue => ex
    #pp ex.backtrace
    ex
  end
end

def result_response_command_with_read_preference(type, command, read_preference)
  read_preference_sym = read_preference.downcase.to_sym
  result_response do
    if type == 'admin'
      @admin.command(command, read: read_preference_sym)
    else
      @db.command(command, read: read_preference_sym)
    end
  end
end

def data_members(which = [:primary, :secondaries])
  topology_members = []
  topology_members = @secondaries.collect{|secondary| [secondary, :secondary]} if which.include?(:secondaries)
  topology_members << [@primary, :primary] if which.include?(:primary)
  client_members = topology_members.collect do |resource, member_type|
    client = Mongo::MongoClient.from_uri(resource.object['mongodb_uri'])
    [resource.object['uri'], {client: client, resource: resource, member_type: member_type}]
  end
  Hash[*client_members.flatten(1)]
end

def hash_delta(a, b)
  ary = b.each_pair.collect{|key, value| [key, (value.to_i - a[key].to_i).abs]}
  Hash[*ary.flatten(1)]
end

def get_server_status
  data_members_with_status = @data_members.each_pair.collect{|key, value|
    server_status = value[:client]['admin'].command({serverStatus: 1})
    #pp [value[:client].host_port, server_status]
    [key, value.dup.merge(server_status: server_status)]
  }
  Hash[*data_members_with_status.flatten(1)]
end

def server_status_delta(status_type = 'opcounters')
  @server_status_after = get_server_status
  status_type_delta = @server_status_after.each_pair.collect do |key, value|
    [key, value.merge(status_type => hash_delta(@server_status_before[key][:server_status][status_type], value[:server_status][status_type]))]
  end
  Hash[*status_type_delta.flatten(1)]
end

def occurs_on(member_type, status_type = 'opcounters', op_type = 'query')
  delta = server_status_delta(status_type)
  threshold = STATUS_THRESHOLD[status_type][op_type]
  nodes = delta.each_pair.select{|key, value| (value[status_type][op_type] > threshold)}
  expect(nodes.count).to eq(1)
  expect(nodes.first.last[:member_type]).to eq(member_type)
end

def members_by_type(member_type)
  case member_type
    when 'primary'
      return [@primary]
    when 'secondary'
      return @secondaries
    when 'arbiter'
      return @arbiters
  end
end

def await_replication(coll)
  coll.insert({a: 0}, w: @n)
end

def setup_topology_and_client(orchestration, preset, id = nil)
  configuration = {orchestration: orchestration, request_content: {preset: preset}}
  configuration[:request_content][:id] = id if id
  @topology = @mo.configure(configuration)
  @topology.reset
  $topologies[@topology.object['id']] = @topology
  @mongodb_uri = @topology.object['mongodb_uri']
  case orchestration
    when 'servers'
      @client = Mongo::MongoClient.from_uri(@mongodb_uri)
      @n = 1
    when 'replica_sets'
      @client = Mongo::MongoReplicaSetClient.from_uri(@mongodb_uri)
      %w[servers primary secondaries arbiters hidden].each do |name|
        instance_variable_set("@#{name}", @topology.send(name))
      end
      @n = 1 + @secondaries.count
    when 'sharded_clusters'
      @client = Mongo::MongoShardedClient.from_uri(@mongodb_uri)
      @routers = @topology.routers
      @n = 1
  end
  @client.drop_database(TEST_DB)
  @admin = @client['admin']
  @db = @client[TEST_DB]
  @coll = @db[TEST_COLL]
  @ordinal = 1
  await_replication(@coll) if orchestration == 'replica_sets'
end

Before do |scenario|
  @mo ||= Mongo::Orchestration::Service.new
end

After do |scenario|
  @topology.destroy if @topology && !ENV['TOPOLOGY_NO_DESTROY']
end

Transform /^(-?\d+)$/ do |number|
  number.to_i
end

Given(/^a (standalone server|replica set|sharded cluster) with preset (\w+)$/) do |topology_type, preset|
  topology_resource = {"standalone server" => "servers",
                      "replica set" => "replica_sets",
                      "sharded cluster" => "sharded_clusters"}
  resource_type = topology_resource[topology_type]
  setup_topology_and_client(resource_type, preset + '.json', "#{resource_type}_#{preset}")
  @server = @topology
end

Given(/^a document written to (?:the server|all data\-bearing members|the cluster)$/) do
  @result = with_rescue do
    @coll.insert({a: @ordinal}, w: @n)
  end
end

Given(/^a replica-set client with a seed from (?:a|the) (primary|secondary|arbiter)$/) do |member_type|
  seed = members_by_type(member_type).first.object['uri']
  mongodb_uri = "mongodb://#{seed}/?replicaSet=#{@topology.object['id']}"
  @client = Mongo::MongoReplicaSetClient.from_uri(mongodb_uri)
end

Given(/^some documents written to all data\-bearing members$/) do
  @coll.insert(TEST_DOCS, w: @n)
end

Given(/^some geo documents written to all data\-bearing members$/) do
  step "some documents written to all data-bearing members"
end

Given(/^a geo (2d) index$/) do |geo_index_type|
  @coll.create_index([['coordinates', geo_index_type]]);
end

Given(/^a geo (geoHaystack) index$/) do |geo_index_type|
  @coll.create_index({ coordinates: geo_index_type, state: 1 }, { bucketSize: 1 });
end

When(/^there is no primary$/) do # review - configuration specific
  @arbiters.each{|server| server.stop}
  @primary.stop
end

When(/^there are no secondaries$/) do
  @secondaries.each{|server| server.stop}
end

When(/^I (stop|start|restart) the server$/) do |operation|
  @server.send(operation)
end

When(/^I command the primary to step down$/) do
  expect(@primary.stepdown.ok).to be true
end

When(/^I (stop|start|restart) (?:a|the) (primary|secondary|arbiter)$/) do |operation, member_type| # review - configuration specific
  members_by_type(member_type).first.send(operation)
end

When(/^I (stop|start|restart) router (A|B)$/) do |operation, router|
  router_pos = {'A' => :first, 'B' => :last}[router]
  @routers.send(router_pos).send(operation)
end

When(/^I track server status on (all data members|the primary|secondaries)$/) do |which_text|
  which_map = {'all data members' => [:primary, :secondaries],
               'the primary' => [:primary],
               'secondaries' => [:secondaries]}
  @data_members = data_members(which_map[which_text])
  @server_status_before = get_server_status
end

When(/^I insert a document$/) do
  @result = with_rescue do
    @coll.insert({a: @ordinal})
  end
end

When(/^I insert a document with retries$/) do
  @result = rescue_connection_failure_and_retry do
    @coll.insert({a: @ordinal})
  end
end

When(/^I (insert|update|delete) a document with the write concern \{ ?“w”: <nodes(?: \+ )?(\d+)?>(?:, “timeout”: )?(\d+)?\}$/) do |operation, plus_nodes, timeout|
  @result = with_rescue do
    options = {:w => @n + (plus_nodes || 0)}
    options.merge!(:wtimeout => timeout) if timeout
    @coll.insert({a: @ordinal}, w: @n) unless operation == 'insert'
    case operation
      when 'insert'
        @coll.insert({a: @ordinal}, options)
      when 'update'
        @coll.update({a: @ordinal}, {}, options.merge(upsert: true))
      when 'delete'
        @coll.remove({a: @ordinal}, options)
    end
  end
end

When(/^I query( with default read preference)?$/) do |arg1|
  @result = with_rescue do
    find_one_ordinal
  end
end

When(/^I query with retries$/) do
  @result = rescue_connection_failure_and_retry do
    find_one_ordinal
  end
end

When(/^I query with read\-preference (\w+)$/) do |read_preference|
  read_preference_sym = read_preference.downcase.to_sym
  @result = with_rescue do
    find_one_ordinal(read: read_preference_sym)
  end
end

When(/^I query with retries and read\-preference (\w+)$/) do |read_preference|
  read_preference_sym = read_preference.downcase.to_sym
  @result = rescue_connection_failure_and_retry do
    find_one_ordinal(read: read_preference_sym)
  end
end

When(/^I query with read\-preference (\w+) and tag sets (.*)$/) do |read_preference, tag_sets|
  @tag_sets = JSON.parse(tag_sets)
  read_preference_sym = read_preference.downcase.to_sym
  @result = with_rescue do
    find_one_ordinal(read: read_preference_sym, tag_sets: @tag_sets)
  end
end

When(/^I query with read\-preference (\w+) and batch size (\d+)$/) do  |read_preference, batch_size|
  read_preference_sym = read_preference.downcase.to_sym
  @cursor = @coll.find({}, read: read_preference_sym, batch_size: batch_size)
end

When(/^I get (\d+) docs$/) do |count|
  @result = with_rescue do
    @docs = count.times.collect{@cursor.next}
  end
end

When(/^I close the cursor$/) do
  @result = with_rescue do
    @cursor.close
  end
end

When(/^I run a (\w+) (\w+) command with read\-preference (\w+) and with example (.*)$/) do |type, name, read_preference, example|
  command = JSON.parse(example)
  command[name] = TEST_COLL if COLL_COMMANDS.include?(name)
  case name
    when"group"
      command["group"]["ns"] = TEST_COLL
      command["group"]["$reduce"] = BSON::Code.new(command["group"]["$reduce"])
    when "eval"
      command["eval"] = BSON::Code.new(command["eval"])
  end
  result_response_command_with_read_preference(type, command, read_preference)
end

When(/^I run a geonear command with read\-preference (\w+)$/) do |read_preference|
  command = {geoNear: TEST_COLL, near: [-73.9667,40.78], maxDistance: 1000}
  result_response_command_with_read_preference('normal', command, read_preference)
end

When(/^I run a geosearch command with read\-preference (\w+)$/) do |read_preference|
  command = {geoSearch: TEST_COLL, near: [-73.9667,40.78], maxDistance: 1}
  result_response_command_with_read_preference('normal', command, read_preference)
end

When(/^I run a map\-reduce with field out value inline true and with read\-preference (\w+)$/) do |read_preference|
  command = {mapReduce: TEST_COLL,
             map: BSON::Code.new("function(){emit('a',this.a)}"),
             reduce: BSON::Code.new("function(key,values){return Array.sum(values)}"),
             out: {inline: true}}
  result_response_command_with_read_preference('normal', command, read_preference)
end

When(/^I run a map\-reduce with field out value other than inline and with read\-preference (\w+)$/) do |read_preference|
  command = {mapReduce: TEST_COLL,
             map: BSON::Code.new("function(){emit('a',this.a)}"),
             reduce: BSON::Code.new("function(key,values){return Array.sum(values)}"),
             out: TEST_COLL_OUT}
  result_response_command_with_read_preference('normal', command, read_preference)
end

When(/^I run an aggregate with \$out and with read\-preference (\w+)$/) do |read_preference|
  command = {aggregate: TEST_COLL,
             'pipeline' => [{'$group' => {'_id' => '$state', 'count' => {'$sum' => 1}}}, {'$out' => TEST_COLL_OUT}]} # RUBY-804
  result_response_command_with_read_preference('normal', command, read_preference)
end

When(/^I run an aggregate without \$out and with read\-preference (\w+)$/) do |read_preference|
  command = {aggregate: TEST_COLL,
             'pipeline' => [{'$group' => {'_id' => '$state', 'count' => {'$sum' => 1}}}]} # RUBY-804
  result_response_command_with_read_preference('normal', command, read_preference)
end

When(/^I execute an (ordered|unordered) bulk write operation ?(with a duplicate key and)? with the write concern \{“w”: <nodes(?: \+ )?(\d+)?>(?:, “timeout”: )?(\d+)?\}$/) do |order, duplicate, plus_nodes, timeout|
  write_concern = {:w => @n + (plus_nodes || 0)}
  write_concern.merge!(:wtimeout => timeout) if timeout
  @coll.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
  bulk = (order == 'ordered') ? @coll.initialize_ordered_bulk_op : @coll.initialize_unordered_bulk_op
  bulk.insert({:a => 1})
  bulk.find({:a => 2}).upsert.update({'$set' => {:a => 2}})
  bulk.insert({:a => 1}) if duplicate
  bulk.find({:a => 1}).remove_one
  @result = begin
    @response = bulk.execute(write_concern)
  rescue Mongo::BulkWriteError => ex
    ex
  end
end

When(/^I remove all documents from the collection$/) do
  @coll.remove
end

Then(/^the insert succeeds$/) do
  expect(@result).to be_instance_of(BSON::ObjectId)
  expect(find_one_ordinal).not_to be_nil
  @ordinal += 1
end

Then(/^the insert fails$/) do
  expect(@result).to be_kind_of(Exception)
end

Then(/^the write operation suceeeds$/) do
  expect(@result).not_to be_kind_of(Exception)
  @ordinal += 1
end

Then(/^the write operation fails write concern$/) do
  expect(@result).to be_instance_of(Mongo::WriteConcernError)
  @ordinal += 1
end

Then(/^the bulk write operation fails$/) do
  expect(@result).to be_instance_of(Mongo::BulkWriteError)
end

Then(/^the bulk write operation succeeds$/) do
  expect(@result).not_to be_instance_of(Mongo::BulkWriteError)
  @ordinal += 1
end

Then(/^the result includes a (write|write concern) error$/) do |write_error_type|
  expect(@result.result['writeErrors']).to be_truthy if write_error_type == 'write'
  expect(@result.result['writeConcernError']).to be_truthy if write_error_type == 'write concern'
end

Then(/^the query succeeds$/) do
  expect(@result).not_to be_kind_of(Exception)
  expect(@result).to be_instance_of(BSON::OrderedHash)
end

Then(/^the query fails$/) do
  expect(@result).to be_kind_of(Exception)
end

Then(/^the query fails with error "(.*?)"$/) do |message|
  expect(@result).to be_kind_of(Exception)
  pattern = message.downcase.gsub(/<tags sets>/, @tag_sets.inspect)
  expect(@result.message.downcase).to match(pattern)
end

Then(/^the (query|getmore|command) occurs on (a|the) (primary|secondary)$/) do |operation, article, member_type|
  occurs_on(member_type.to_sym, 'opcounters', operation)
end

Then(/^the (kill cursors) occurs on (a|the) (primary|secondary)$/) do |operation, article, member_type|
  occurs_on(member_type.to_sym, 'cursors', 'totalOpen')
end

Then(/^the get succeeds$/) do
  expect(@result).not_to be_kind_of(Exception)
  expect(@result).to be_instance_of(Array)
end

Then(/^the get fails$/) do
  expect(@result).to be_kind_of(Exception)
end

Then(/^the close succeeds$/) do
  expect(@result).not_to be_kind_of(Exception)
end

Then(/^dump$/) do
  pp @result
  pp @response
  pp @coll.find.to_a
end

