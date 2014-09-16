$ORCH_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', 'orchestration'))
$LOAD_PATH.unshift($ORCH_DIR)
$LIB_DIR = File.absolute_path(File.join(File.dirname(__FILE__), '..', '..', '..', 'lib'))
$LOAD_PATH.unshift($LIB_DIR)

require 'bson'
require 'mongo'
require 'mongo_orchestration'

TEST_DB = 'test'
TEST_COLL = 'test'
OPCOUNTER_QUERY = 1
OPCOUNTER_COMMAND = 2

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

def opcounters(client, field = 'query')
  client['admin'].command({serverStatus: 1})['opcounters'][field]
end

def opcounter_count(client, field = 'query')
  direct_client = Mongo::MongoClient.new(client.read_pool.host, client.read_pool.port)
  queries_before = opcounters(direct_client, field)
  #assert_nothing_raised do
    yield
  #end
  queries_after = opcounters(direct_client, field)
  queries_after - queries_before
end

def result_opcount_response(client, field = 'query')
  response = nil
  count = nil
  result = begin
    count = opcounter_count(client, field) do
      response = yield
    end
    response
  rescue => ex
    ex
  end
  [result, count, response]
end

Before do |scenario|
  @mo ||= Mongo::Orchestration::Service.new
end

After do |scenario|
  @cluster.destroy if @cluster
end

Given(/^a cluster in the standalone server configuration$/) do
  @cluster = @mo.configure({orchestration: "servers", request_content: {id: "standalone", preset: "basic.json"} })
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
  @cluster = @mo.configure({orchestration: "sharded_clusters", request_content: {id: "sharded_cluster_1", preset: "basic.json"} })
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

Then(/^the insert fails write concern$/) do
  assert(@result.is_a?(Mongo::WriteConcernError))
  @ordinal += 1
end

When(/^I update a document with the write concern \{ “w”: <nodes \+ (\d+)>, “timeout”: (\d+)\}$/) do |arg1, arg2|
  @result = with_rescue do
    @coll.update({a: @ordinal}, {}, w: @n + 1, wtimeout: 1, upsert: true)
  end
end

Then(/^the update fails write concern$/) do
  assert(@result.is_a?(Mongo::WriteConcernError))
  @ordinal += 1
end

When(/^I delete a document with the write concern \{ “w”: <nodes \+ (\d+)>, “timeout”: (\d+)\}$/) do |arg1, arg2|
  @coll.insert({a: @ordinal}, w: @n)
  @result = with_rescue do
    @coll.remove({a: @ordinal}, w: @n + 1, wtimeout: 1)
  end
end

Then(/^the delete fails write concern$/) do
  assert(@result.is_a?(Mongo::WriteConcernError))
  @ordinal += 1
end

Given(/^a document written to all data\-bearing members$/) do
  @result = with_rescue do
    @coll.insert({a: @ordinal}, w: @n)
  end
end

Given(/^a client with read\-preference (\w+)$/) do |read_preference|
  read_preference_sym = read_preference.downcase.to_sym
  @client = Mongo::MongoClient.from_uri(@mongodb_uri, read: read_preference_sym)
  # if read_preference =~ /PRIMARY/
  #   assert_equal(@primary.object['uri'], @client.read_pool.address)
  # else
  #   assert(@primary.object['uri'] != @client.read_pool.address)
  # end
end

Given(/^a client with read\-preference (\w+) and tag sets (.*)$/) do |read_preference, tag_sets|
  @tag_sets = JSON.parse(tag_sets)
  read_preference_sym = read_preference.downcase.to_sym
  @client = Mongo::MongoClient.from_uri(@mongodb_uri, read: read_preference_sym, tag_sets: @tag_sets)
  # if read_preference =~ /PRIMARY/
  #   assert_equal(@primary.object['uri'], @client.read_pool.address)
  # else
  #   assert(@primary.object['uri'] != @client.read_pool.address)
  # end
end

When(/^I read with opcounter tracking$/) do
  @result, @count, @response = result_opcount_response(@client) do
    @client[TEST_DB][TEST_COLL].find_one({"a" => @ordinal})
  end
end

When(/^I read$/) do
  @result = with_rescue do
    @client[TEST_DB][TEST_COLL].find_one({"a" => @ordinal})
  end
end

Then(/^the read occurs on the primary$/) do
  assert(OPCOUNTER_QUERY == @count)
end

Then(/^the read occurs on a secondary$/) do
  assert_equal(OPCOUNTER_QUERY, @count)
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

When(/^I run with opcounter tracking a (\w+) command with example (.*)$/) do |name, example|
  command = JSON.parse(example)
  command[name] = TEST_COLL if ["aggregate", "collStats", "count", "mapReduce", "parallelCollectionScan"].include?(name)
  if name == "group"
    command["group"]["ns"] = TEST_COLL
    command["group"]["$reduce"] = BSON::Code.new(command["group"]["$reduce"])
  elsif name == "mapReduce"
    command["map"] = BSON::Code.new(command["map"])
    command["reduce"] = BSON::Code.new(command["reduce"])
  end
  @result, @count, @response = result_opcount_response(@client, 'command') do
    @client[TEST_DB].command(command)
  end
end

Then(/^the command occurs on a secondary$/) do
  assert_equal(OPCOUNTER_COMMAND, @count)
end
