$ORCH_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', 'orchestration'))
$LOAD_PATH.unshift($ORCH_DIR)
$LIB_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', '..', 'lib'))
$LOAD_PATH.unshift($LIB_DIR)

require 'bson'
require 'mongo'
require 'mongo_orchestration'

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
OPCOUNTER_QUERY = 1
OPCOUNTER_COMMAND = 2

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

def setup_db_coll
  @client.drop_database(TEST_DB)
  @db = @client[TEST_DB]
  @coll = @db[TEST_COLL]
  @ordinal = 1
end

def rescue_connection_failure_and_retry(max_retries=30)
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

def await_replication(coll)
  coll.insert({a: 0}, w: @n)
end

def with_rescue(exception_class = Exception)
  begin
    yield
  rescue exception_class => ex
    ex
  end
end

def data_members
  cluster_members = @cluster.secondaries.collect{|secondary| [secondary, :secondary]} << [@cluster.primary, :primary]
  client_members = cluster_members.collect do |resource, member_type|
    object = resource.object
    client = Mongo::MongoClient.from_uri(object['mongodb_uri'])
    [object['uri'], {client: client, resource: resource, member_type: member_type}]
  end
  Hash[*client_members.flatten(1)]
end

def hash_delta(a, b)
  ary = b.each_pair.collect{|key, value| [key, value - a[key]]}
  Hash[*ary.flatten(1)]
end

def get_opcounters
  @data_members = data_members
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
  threshold = (op_type == 'command') ? 1 : 0
  opnodes = @opcounters_delta.each_pair.select{|key, value| (value[:opcounters][op_type] > threshold)}
  assert_equal(1, opnodes.count)
  assert_equal(member_type, opnodes.first.last[:member_type])
end

def result_response
  response = nil
  result = begin
    response = yield
  rescue => ex
    #pp ex.backtrace
    ex
  end
  [result, response]
end

def result_response_command_with_read_preference(command, read_preference)
  read_preference_sym = read_preference.downcase.to_sym
  @result, @response = result_response do
    @client[TEST_DB].command(command, read: read_preference_sym)
  end
end

Before do |scenario|
  @mo ||= Mongo::Orchestration::Service.new
end

After do |scenario|
  @cluster.destroy if @cluster
end

Given(/^a cluster in the standalone server configuration$/) do
  @cluster = @mo.configure({orchestration: "servers", request_content: {id: "standalone_basic", preset: "basic.json"} })
  @mongodb_uri = @cluster.object['mongodb_uri']
  @client = Mongo::MongoClient.from_uri(@mongodb_uri)
  setup_db_coll
  @n = 1
  @server = @cluster
end

Given(/^a basic replica set$/) do
  @cluster = @mo.configure({orchestration: "replica_sets", request_content: {id: "replica_set_basic", preset: "basic.json"} })
  @mongodb_uri = @cluster.object['mongodb_uri']
  @client = Mongo::MongoReplicaSetClient.from_uri(@mongodb_uri)
  setup_db_coll
  await_replication(@coll)
  @primary = @cluster.primary
  @n = @cluster.object['members'].count
end

Given(/^an arbiter replica set$/) do
  @cluster = @mo.configure({orchestration: "replica_sets", request_content: {id: "replica_set_arbiter", preset: "arbiter.json"} })
  @mongodb_uri = @cluster.object['mongodb_uri']
  @client = Mongo::MongoReplicaSetClient.from_uri(@mongodb_uri)
  setup_db_coll
  await_replication(@coll)
  @primary = @cluster.primary
  @n = @cluster.object['members'].count - 1
end

Given(/^a basic sharded cluster with routers A and B$/) do
  @cluster = @mo.configure({orchestration: "sharded_clusters", request_content: {id: "sharded_cluster_basic", preset: "basic.json"} })
  @mongodb_uri = @cluster.object['mongodb_uri']
  @client = Mongo::MongoShardedClient.from_uri(@mongodb_uri)
  setup_db_coll
  @routers = @cluster.routers
  @n = 1
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

Then(/^the insert succeeds$/) do
  assert(@result.is_a?(BSON::ObjectId))
  assert(@coll.find_one({"a" => @ordinal}))
  @ordinal += 1
end

Then(/^the insert succeeds \(eventually\)$/) do
  step "the insert succeeds"
end

Then(/^the insert fails$/) do
  assert(@result.is_a?(Exception))
  @ordinal += 1
end

When(/^I stop the server$/) do
  @server.stop
end

When(/^I start the server$/) do
  @server.start
end

When(/^I restart the server$/) do
  @server.restart
end

When(/^I command the primary to step down$/) do
  assert(@primary.stepdown.ok)
end

When(/^I stop router A$/) do
  @routers.first.stop
end

When(/^I start router A$/) do
  @routers.first.start
end

When(/^I restart router A$/) do
  @routers.first.restart
end

When(/^I stop router B$/) do
  @routers.last.stop
end

When(/^I start router B$/) do
  @routers.last.start
end

When(/^I restart router B$/) do
  @routers.last.restart
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

Then(/^the write operation fails write concern$/) do
  assert(@result.is_a?(Mongo::WriteConcernError))
  @ordinal += 1
end

Given(/^a document written to all data\-bearing members$/) do
  @result = with_rescue do
    @coll.insert({a: @ordinal}, w: @n)
  end
end

When(/^I track opcounters$/) do
  @opcounters_before = get_opcounters
end

When(/^I read$/) do
  @result = with_rescue do
    @client[TEST_DB][TEST_COLL].find_one({"a" => @ordinal})
  end
end

When(/^I read with read\-preference (\w+)$/) do  |read_preference|
  read_preference_sym = read_preference.downcase.to_sym
  @result = with_rescue do
    @client[TEST_DB][TEST_COLL].find_one({"a" => @ordinal}, read: read_preference_sym)
  end
end

When(/^I read with read\-preference (\w+) and tag sets (.*)$/) do |read_preference, tag_sets|
  @tag_sets = JSON.parse(tag_sets)
  read_preference_sym = read_preference.downcase.to_sym
  @result = with_rescue do
    @client[TEST_DB][TEST_COLL].find_one({"a" => @ordinal}, read: read_preference_sym, tag_sets: @tag_sets)
  end
end

Then(/^the read occurs on the primary$/) do
  occurs_on(:primary, 'query')
end

Then(/^the read occurs on a secondary$/) do
  occurs_on(:secondary, 'query')
end

When(/^there is no primary$/) do
  @cluster.arbiters.first.stop
  @cluster.primary.stop
end

When(/^there are no secondaries$/) do
  @cluster.secondaries.first.stop
end

Then(/^the read succeeds$/) do
  assert(!@result.is_a?(Exception) && @result.is_a?(Hash))
end

Then(/^the read fails$/) do
  assert(@result.is_a?(Mongo::ConnectionFailure))
end

Then(/^the read fails with error "(.*?)"$/) do |message|
  assert(@result.is_a?(Exception))
  pattern = message.downcase.gsub(/<tags sets>/, @tag_sets.inspect)
  assert_match(pattern, @result.message.downcase)
end

When(/^I run a (\w+) command with read\-preference (\w+) and with example (.*)$/) do |name, read_preference, example|
  command = JSON.parse(example)
  command[name] = TEST_COLL if ["aggregate", "collStats", "count", "mapReduce", "parallelCollectionScan"].include?(name)
  if name == "group"
    command["group"]["ns"] = TEST_COLL
    command["group"]["$reduce"] = BSON::Code.new(command["group"]["$reduce"])
  end
  result_response_command_with_read_preference(command, read_preference)
end

Then(/^the command occurs on a secondary$/) do
  occurs_on(:secondary, 'command')
end

Then(/^the command occurs on the primary$/) do
  occurs_on(:primary, 'command')
end

Given(/^some geo documents written to all data\-bearing members$/) do
  @coll.insert(TEST_DOCS, :w => @n)
end

Given(/^a geo (\w+) index$/) do |geo_index_type|
  @coll.create_index([['coordinates', geo_index_type]]);
end

When(/^I run a geonear command with read\-preference (\w+)$/) do |read_preference|
  command = {geoNear: TEST_COLL, near: [-73.9667,40.78], maxDistance: 1000}
  result_response_command_with_read_preference(command, read_preference)
end

Given(/^some documents written to all data\-bearing members$/) do
  @coll.insert(TEST_DOCS, :w => @n)
end

When(/^I run a map\-reduce with field out value inline true and with read\-preference (\w+)$/) do |read_preference|
  command = {mapReduce: TEST_COLL,
             map: BSON::Code.new("function(){emit('a',this.a)}"),
             reduce: BSON::Code.new("function(key,values){return Array.sum(values)}"),
             out: {inline: true}}
  result_response_command_with_read_preference(command, read_preference)
end

When(/^I run a map\-reduce with field out value other than inline and with read\-preference (\w+)$/) do |read_preference|
  command = {mapReduce: TEST_COLL,
             map: BSON::Code.new("function(){emit('a',this.a)}"),
             reduce: BSON::Code.new("function(key,values){return Array.sum(values)}"),
             out: TEST_COLL_OUT}
  result_response_command_with_read_preference(command, read_preference)
end

When(/^I run an aggregate with \$out and with read\-preference (\w+)$/) do |read_preference|
  command = {aggregate: TEST_COLL,
             'pipeline' => [{'$group' => {'_id' => '$state', 'count' => {'$sum' => 1}}}, {'$out' => TEST_COLL_OUT}]} # RUBY-804
  result_response_command_with_read_preference(command, read_preference)
end

When(/^I run an aggregate without \$out and with read\-preference (\w+)$/) do |read_preference|
  command = {aggregate: TEST_COLL,
             'pipeline' => [{'$group' => {'_id' => '$state', 'count' => {'$sum' => 1}}}]} # RUBY-804
  result_response_command_with_read_preference(command, read_preference)
end
