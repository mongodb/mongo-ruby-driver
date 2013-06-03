# Copyright (C) 2013 10gen Inc.
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

require 'test_helper'
require 'digest/md5'
require 'stringio'
require 'logger'

class TestPKFactory
  def create_pk(row)
    row['_id'] ||= BSON::ObjectId.new
    row
  end
end

class DBTest < Test::Unit::TestCase

  include Mongo

  @@client  = standard_connection
  @@db    = @@client.db(MONGO_TEST_DB)
  @@users = @@db.collection('system.users')
  @@version = @@client.server_version

  def test_close
    @@client.close
    assert !@@client.connected?
    begin
      @@db.collection('test').insert('a' => 1)
      fail "expected 'NilClass' exception"
    rescue => ex
      assert_match(/NilClass/, ex.to_s)
    ensure
      @@db = standard_connection.db(MONGO_TEST_DB)
      @@users = @@db.collection('system.users')
    end
  end

  def test_create_collection
    col = @@db.create_collection('foo')
    assert_equal @@db['foo'].name, col.name

    col = @@db.create_collection(:foo)
    assert_equal @@db['foo'].name, col.name

    @@db.drop_collection('foo')
  end

  def test_get_and_drop_collection
    db = @@client.db(MONGO_TEST_DB, :strict => true)
    db.create_collection('foo')
    assert db.collection('foo')
    assert db.drop_collection('foo')

    db.create_collection(:foo)
    assert db.collection(:foo)
    assert db.drop_collection(:foo)
  end

  def test_logger
    output = StringIO.new
    logger = Logger.new(output)
    logger.level = Logger::DEBUG
    conn = standard_connection(:logger => logger)
    assert_equal logger, conn.logger

    conn.logger.debug 'testing'
    assert output.string.include?('testing')
  end

  def test_full_coll_name
    coll = @@db.collection('test')
    assert_equal "#{MONGO_TEST_DB}.test", @@db.full_collection_name(coll.name)
  end

  def test_collection_names
    @@db.collection("test").insert("foo" => 5)
    @@db.collection("test.mike").insert("bar" => 0)

    colls = @@db.collection_names()
    assert colls.include?("test")
    assert colls.include?("test.mike")
    colls.each { |name|
      assert !name.include?("$")
    }
  end

  def test_collections
    @@db.collection("test.durran").insert("foo" => 5)
    @@db.collection("test.les").insert("bar" => 0)

    colls = @@db.collections()
    assert_not_nil colls.select { |coll| coll.name == "test.durran" }
    assert_not_nil colls.select { |coll| coll.name == "test.les" }
    assert_equal [], colls.select { |coll| coll.name == "does_not_exist" }

    assert_kind_of Collection, colls[0]
  end

  def test_pk_factory
    db = standard_connection.db(MONGO_TEST_DB, :pk => TestPKFactory.new)
    coll = db.collection('test')
    coll.remove

    insert_id = coll.insert('name' => 'Fred', 'age' => 42)
    # new id gets added to returned object
    row = coll.find_one({'name' => 'Fred'})
    oid = row['_id']
    assert_not_nil oid
    assert_equal insert_id, oid

    oid = BSON::ObjectId.new
    data = {'_id' => oid, 'name' => 'Barney', 'age' => 41}
    coll.insert(data)
    row = coll.find_one({'name' => data['name']})
    db_oid = row['_id']
    assert_equal oid, db_oid
    assert_equal data, row

    coll.remove
  end

  def test_pk_factory_reset
    conn = standard_connection
    db   = conn.db(MONGO_TEST_DB)
    db.pk_factory = Object.new # first time
    begin
      db.pk_factory = Object.new
      fail "error: expected exception"
    rescue => ex
      assert_match(/Cannot change/, ex.to_s)
    ensure
      conn.close
    end
  end

  def test_command
    assert_raise OperationFailure do
      @@db.command({:non_command => 1}, :check_response => true)
    end

    result = @@db.command({:non_command => 1}, :check_response => false)
    assert !Mongo::Support.ok?(result)
  end

  def test_error
    @@db.reset_error_history
    assert_nil @@db.get_last_error['err']
    assert !@@db.error?
    assert_nil @@db.previous_error

    @@db.command({:forceerror => 1}, :check_response => false)
    assert @@db.error?
    assert_not_nil @@db.get_last_error['err']
    assert_not_nil @@db.previous_error

    @@db.command({:forceerror => 1}, :check_response => false)
    assert @@db.error?
    assert @@db.get_last_error['err']
    prev_error = @@db.previous_error
    assert_equal 1, prev_error['nPrev']
    assert_equal prev_error["err"], @@db.get_last_error['err']

    @@db.collection('test').find_one
    assert_nil @@db.get_last_error['err']
    assert !@@db.error?
    assert @@db.previous_error
    assert_equal 2, @@db.previous_error['nPrev']

    @@db.reset_error_history
    assert_nil @@db.get_last_error['err']
    assert !@@db.error?
    assert_nil @@db.previous_error
  end

  def test_check_command_response
    command = {:forceerror => 1}
    raised = false
    begin
      @@db.command(command)
    rescue => ex
      raised = true
      assert ex.message.include?("forced error"),
        "error message does not contain 'forced error'"
      assert_equal 10038, ex.error_code

      if @@version >= "2.1.0"
        assert_equal 10038, ex.result['code']
      else
        assert_equal 10038, ex.result['assertionCode']
      end
    ensure
      assert raised, "No assertion raised!"
    end
  end

  def test_last_status
    @@db['test'].remove
    @@db['test'].save("i" => 1)

    @@db['test'].update({"i" => 1}, {"$set" => {"i" => 2}})
    assert @@db.get_last_error()["updatedExisting"]

    @@db['test'].update({"i" => 1}, {"$set" => {"i" => 500}})
    assert !@@db.get_last_error()["updatedExisting"]
  end

  def test_text_port_number_raises_no_errors
    client = standard_connection
    db   = client[MONGO_TEST_DB]
    db.collection('users').remove
  end

  def test_stored_function_management
    @@db.add_stored_function("sum", "function (x, y) { return x + y; }")
    assert_equal @@db.eval("return sum(2,3);"), 5
    assert @@db.remove_stored_function("sum")
    assert_raise OperationFailure do
      @@db.eval("return sum(2,3);")
    end
  end

  def test_eval
    @@db.eval("db.system.save({_id:'hello', value: function() { print('hello'); } })")
    assert_equal 'hello', @@db['system'].find_one['_id']
  end

  if @@version >= "1.3.5"
    def test_db_stats
      stats = @@db.stats
      assert stats.has_key?('collections')
      assert stats.has_key?('dataSize')
    end
  end

  context "database profiling" do
    setup do
      @db  = @@client[MONGO_TEST_DB]
      @coll = @db['test']
      @coll.remove
      @r1 = @coll.insert('a' => 1) # collection not created until it's used
    end

    should "set default profiling level" do
      assert_equal :off, @db.profiling_level
    end

    should "change profiling level" do
      @db.profiling_level = :slow_only
      assert_equal :slow_only, @db.profiling_level
      @db.profiling_level = :off
      assert_equal :off, @db.profiling_level
      @db.profiling_level = :all
      assert_equal :all, @db.profiling_level
      begin
        @db.profiling_level = :medium
        fail "shouldn't be able to do this"
      rescue
      end
    end

    should "return profiling info" do
      @db.profiling_level = :all
      @coll.find()
      @db.profiling_level = :off

      info = @db.profiling_info
      assert_kind_of Array, info
      assert info.length >= 1
      first = info.first
      assert_kind_of Time, first['ts']
      assert_kind_of Numeric, first['millis']
    end

    should "validate collection" do
      doc = @db.validate_collection(@coll.name)
      if @@version >= "1.9.1"
        assert doc['valid']
      else
        assert doc['result']
      end
    end

  end
end
