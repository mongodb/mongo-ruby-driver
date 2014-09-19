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

$ORCH_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', 'orchestration'))
$LOAD_PATH.unshift($ORCH_DIR)
$LIB_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', '..', 'lib'))
$LOAD_PATH.unshift($LIB_DIR)

require 'bson'
require 'mongo'
require 'mongo_orchestration'

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
OPCOUNTER_QUERY_THRESHOLD = 0
OPCOUNTER_COMMAND_THRESHOLD = 1
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

public

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

def data_members
  cluster_members = @secondaries.collect{|secondary| [secondary, :secondary]} << [@primary, :primary]
  client_members = cluster_members.collect do |resource, member_type|
    client = Mongo::MongoClient.from_uri(resource.object['mongodb_uri'])
    [resource.object['uri'], {client: client, resource: resource, member_type: member_type}]
  end
  Hash[*client_members.flatten(1)]
end

def hash_delta(a, b)
  ary = b.each_pair.collect{|key, value| [key, value - a[key]]}
  Hash[*ary.flatten(1)]
end

def get_opcounters
  data_members_with_opcounters = @data_members.each_pair.collect{|key, value|
    opcounters = value[:client]['admin'].command({serverStatus: 1})['opcounters']
    #pp [value[:client].host_port, opcounters]
    [key, value.dup.merge(opcounters: opcounters)]
  }
  Hash[*data_members_with_opcounters.flatten(1)]
end

def delta_opcounters
  @opcounters_after = get_opcounters
  @opcounters_delta = @opcounters_after.each_pair.collect do |key, value|
    [key, value.merge(opcounters: hash_delta(@opcounters_before[key][:opcounters], value[:opcounters]))]
  end
  @opcounters_delta = Hash[*@opcounters_delta.flatten(1)]
end

def occurs_on(member_type, op_type = 'query')
  delta_opcounters
  #@opcounters_delta.each_pair{|key, value| pp [key, value[:opcounters], value[:member_type]]}
  threshold = (op_type == 'command') ? OPCOUNTER_COMMAND_THRESHOLD : OPCOUNTER_QUERY_THRESHOLD
  opnodes = @opcounters_delta.each_pair.select{|key, value| (value[:opcounters][op_type] > threshold)}
  assert_equal(1, opnodes.count)
  assert_equal(member_type, opnodes.first.last[:member_type])
end

def await_replication(coll)
  coll.insert({a: 0}, w: @n)
end

def setup_cluster_and_client(orchestration, preset, id = nil)
  configuration = {orchestration: orchestration, request_content: {preset: preset}}
  configuration[:request_content][:id] = id if id
  @cluster = @mo.configure(configuration)
  @mongodb_uri = @cluster.object['mongodb_uri']
  case orchestration
    when 'servers'
      @client = Mongo::MongoClient.from_uri(@mongodb_uri)
      @n = 1
    when 'replica_sets'
      @client = Mongo::MongoReplicaSetClient.from_uri(@mongodb_uri)
      @primary = @cluster.primary
      @secondaries = @cluster.secondaries
      @arbiters = @cluster.arbiters
      @n = 1 + @secondaries.count
    when 'sharded_clusters'
      @client = Mongo::MongoShardedClient.from_uri(@mongodb_uri)
      @routers = @cluster.routers
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
  @cluster.destroy if @cluster && !('1' == ENV['CLUSTER_DESTROY'])
end

Transform /^(-?\d+)$/ do |number|
  number.to_i
end

Given(/^a (standalone server|replica set|sharded cluster) with preset (\w+)$/) do |cluster_type, preset|
  cluster_resource = {"standalone server" => "servers",
                      "replica set" => "replica_sets",
                      "sharded cluster" => "sharded_clusters"}
  resource_type = cluster_resource[cluster_type]
  setup_cluster_and_client(resource_type, preset + '.json', "#{resource_type}_#{preset}")
  @server = @cluster
end

Given(/^a document written to all data\-bearing members$/) do
  @result = with_rescue do
    @coll.insert({a: @ordinal}, w: @n)
  end
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

When(/^there is no primary$/) do
  @arbiters.first.stop
  @primary.stop
end

When(/^there are no secondaries$/) do
  @secondaries.first.stop
end

When(/^I (stop|start|restart) the server$/) do |operation|
  @server.send(operation)
end

When(/^I command the primary to step down$/) do
  assert(@primary.stepdown.ok)
end

When(/^I (stop) the arbiter and the primary$/) do |operation|
  @arbiters.first.stop
  @primary.send(operation)
  @client.refresh #review
end

When(/^I (stop|start|restart) router (A|B)$/) do |operation, router|
  router_pos = {'A' => :first, 'B' => :last}[router]
  @routers.send(router_pos).send(operation)
end

When(/^I track opcounters$/) do
  @data_members = data_members
  @opcounters_before = get_opcounters
end

When(/^I insert a document$/) do
  @result = with_rescue do
    @coll.insert({a: @ordinal})
  end
end

When(/^I insert a document with retries$/) do
  rescue_connection_failure_and_retry do
    @coll.insert({a: @ordinal})
  end
end

When(/^I insert a document with the write concern \{ “w”: <nodes \+ (\d+)>, “timeout”: (\d+)\}$/) do |arg1, arg2|
  @result = with_rescue do
    @coll.insert({a: @ordinal}, w: @n + 1, wtimeout: 1)
  end
end

When(/^I update a document with the write concern \{ “w”: <nodes \+ (\d+)>, “timeout”: (\d+)\}$/) do |arg1, arg2|
  @coll.insert({a: @ordinal}, w: @n)
  @result = with_rescue do
    @coll.update({a: @ordinal}, {}, w: @n + 1, wtimeout: 1, upsert: true)
  end
end

When(/^I delete a document with the write concern \{ “w”: <nodes \+ (\d+)>, “timeout”: (\d+)\}$/) do |arg1, arg2|
  @coll.insert({a: @ordinal}, w: @n)
  @result = with_rescue do
    @coll.remove({a: @ordinal}, w: @n + 1, wtimeout: 1)
  end
end

When(/^I query( with default read preference)?$/) do |arg1|
  @result = with_rescue do
    @coll.find_one({"a" => @ordinal})
  end
end

When(/^I query with read\-preference (\w+)$/) do |read_preference|
  read_preference_sym = read_preference.downcase.to_sym
  @result = with_rescue do
    @coll.find_one({"a" => @ordinal}, read: read_preference_sym)
  end
end

When(/^I query with read\-preference (\w+) and tag sets (.*)$/) do |read_preference, tag_sets|
  @tag_sets = JSON.parse(tag_sets)
  read_preference_sym = read_preference.downcase.to_sym
  @result = with_rescue do
    @coll.find_one({"a" => @ordinal}, read: read_preference_sym, tag_sets: @tag_sets)
  end
end

When(/^I query with read\-preference (\w+) and batch size (\d+)$/) do  |read_preference, batch_size|
  read_preference_sym = read_preference.downcase.to_sym
  @cursor = @coll.find({}, read: read_preference_sym, batch_size: batch_size)
end

When(/^I get (\d+) docs$/) do |count|
  #@secondary ||= Mongo::MongoClient.from_uri(@secondaries.first.object['mongodb_uri'])
  #pp @secondary[TEST_DB].command({serverStatus: 1})['opcounters']
  @result = with_rescue do
    @docs = count.times.collect{@cursor.next}
  end
  #pp @secondary[TEST_DB].command({serverStatus: 1})['opcounters']
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

Then(/^the insert succeeds$/) do
  assert(@result.is_a?(BSON::ObjectId))
  assert(@coll.find_one({"a" => @ordinal}))
  @ordinal += 1
end

Then(/^the insert fails$/) do
  assert(@result.is_a?(Exception))
  @ordinal += 1
end

Then(/^the write operation fails write concern$/) do
  assert(@result.is_a?(Mongo::WriteConcernError))
  @ordinal += 1
end

Then(/^the query succeeds$/) do
  assert(!@result.is_a?(Exception) && @result.is_a?(Hash))
end

Then(/^the query fails$/) do
  assert(@result.is_a?(Mongo::ConnectionFailure))
end

Then(/^the query fails with error "(.*?)"$/) do |message|
  assert(@result.is_a?(Exception))
  pattern = message.downcase.gsub(/<tags sets>/, @tag_sets.inspect)
  assert_match(pattern, @result.message.downcase)
end

Then(/^the (\w+) occurs on (a|the) (primary|secondary)$/) do |operation, article, member_type|
  assert(['query','command'].include?(operation))
  occurs_on(member_type.to_sym, operation)
end

Then(/^the get succeeds$/) do
  #pp @result
  assert(!@result.is_a?(Exception) && @result.is_a?(Array))
end

Then(/^the close succeeds$/) do
  assert(!@result.is_a?(Exception))
end

Then(/^dump$/) do
  pp @result
  pp @response
  pp @coll.find.to_a
end

