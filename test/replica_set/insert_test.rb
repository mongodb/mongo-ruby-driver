# Copyright (C) 2009-2013 MongoDB, Inc.
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

class ReplicaSetInsertTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.from_uri(@uri)
    @version = @client.server_version
    @db = @client.db(TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    @client.close if @conn
  end

  def test_insert
    @coll.save({:a => 20}, :w => 3)

    @rs.primary.stop

    rescue_connection_failure do
      @coll.save({:a => 30}, :w => 1)
    end

    @coll.save({:a => 40}, :w => 1)
    @coll.save({:a => 50}, :w => 1)
    @coll.save({:a => 60}, :w => 1)
    @coll.save({:a => 70}, :w => 1)

    # Restart the old master and wait for sync
    @rs.start
    sleep(5)
    results = []

    rescue_connection_failure do
      @coll.find.each {|r| results << r}
      [20, 30, 40, 50, 60, 70].each do |a|
        assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a}"
      end
    end

    @coll.save({:a => 80}, :w => 3)
    @coll.find.each {|r| results << r}
    [20, 30, 40, 50, 60, 70, 80].each do |a|
      assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a} on second find"
    end
  end

  context "Bulk API CollectionView" do
    setup do
      setup
    end

    should "handle error with deferred write concern error - spec Merging Results" do
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @coll.remove
        @coll.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @coll.initialize_ordered_bulk_op
        bulk.insert({:a => 1})
        bulk.find({:a => 2}).upsert.update({'$set' => {:a => 2}})
        bulk.insert({:a => 1})
        ex = assert_raise BulkWriteError do
          bulk.execute({:w => 5, :wtimeout => 1})
        end
        result = ex.result
        assert_match_document(
            {
                "ok" => 1,
                "n" => 2,
                "writeErrors" => [
                    {
                        "index" => 2,
                        "code" => 11000,
                        "errmsg" => /duplicate key error/,
                    }
                ],
                "writeConcernError" => [
                    {
                        "errmsg" => /waiting for replication timed out|timed out waiting for slaves|timeout/,
                        "code" => 64,
                        "errInfo" => {"wtimeout" => true},
                        "index" => 0
                    },
                    {
                        "errmsg" => /waiting for replication timed out|timed out waiting for slaves|timeout/,
                        "code" => 64,
                        "errInfo" => {"wtimeout" => true},
                        "index" => 1
                    }
                ],
                "code" => 65,
                "errmsg" => "batch item errors occurred",
                "nInserted" => 1
            }, result, "wire_version:#{wire_version}")
      end
      assert_equal 2, @coll.find.to_a.size
    end

    should "handle unordered errors with deferred write concern error - spec Merging Results" do # TODO - spec review
      with_write_commands_and_operations(@db.connection) do |wire_version|
        @coll.remove
        @coll.ensure_index(BSON::OrderedHash[:a, Mongo::ASCENDING], {:unique => true})
        bulk = @coll.initialize_unordered_bulk_op
        bulk.insert({:a => 1})
        bulk.find({:a => 2}).upsert.update({'$set' => {:a => 1}})
        bulk.insert({:a => 3})
        ex = assert_raise BulkWriteError do
          bulk.execute({:w => 5, :wtimeout => 1})
        end
        result = ex.result # unordered varies, don't use assert_bulk_exception
        assert_equal(1, result["ok"], "wire_version:#{wire_version}")
        assert_equal(2, result["n"], "wire_version:#{wire_version}")
        assert(result["nInserted"] >= 1, "wire_version:#{wire_version}")
        assert_equal(65, result["code"], "wire_version:#{wire_version}")
        assert_equal("batch item errors occurred", result["errmsg"], "wire_version:#{wire_version}")
        assert(result["writeErrors"].size >= 1,  "wire_version:#{wire_version}")
        assert(result["writeConcernError"].size >= 1, "wire_version:#{wire_version}") if wire_version >= 2
        assert(@coll.size >= 1, "wire_version:#{wire_version}")
      end
    end

  end

end
