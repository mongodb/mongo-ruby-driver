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
include Mongo

class WriteConcernTest < Test::Unit::TestCase
  context "Write concern propogation: " do
    setup do
      @con = standard_connection
      @db  = @con[MONGO_TEST_DB]
      @col = @db['test-safe']
      @col.create_index([[:a, 1]], :unique => true)
      @col.remove
    end

    #TODO: add write concern tests for remove

    should "propogate write concern options on insert" do
      @col.insert({:a => 1})

      assert_raise_error(OperationFailure, "duplicate key") do
        @col.insert({:a => 1})
      end
    end

    should "allow write concern override on insert" do
      @col.insert({:a => 1})
      @col.insert({:a => 1}, :w => 0)
    end

    should "propogate write concern option on update" do
      @col.insert({:a => 1})
      @col.insert({:a => 2})

      assert_raise_error(OperationFailure, "duplicate key") do
        @col.update({:a => 2}, {:a => 1})
      end
    end

    should "allow write concern override on update" do
      @col.insert({:a => 1})
      @col.insert({:a => 2})
      @col.update({:a => 2}, {:a => 1}, :w => 0)
    end
  end

  context "Write concern error objects" do
    setup do
      @con = standard_connection
      @db  = @con[MONGO_TEST_DB]
      @col = @db['test']
      @col.remove
      @col.insert({:a => 1})
      @col.insert({:a => 1})
      @col.insert({:a => 1})
    end

    should "return object on update" do
      response = @col.update({:a => 1}, {"$set" => {:a => 2}},
                             :multi => true)

      assert response['updatedExisting']
      assert_equal 3, response['n']
    end

    should "return object on remove" do
      response = @col.remove({})
      assert_equal 3, response['n']
    end
  end

  context "Write concern in gridfs" do
    setup do
      @db = standard_connection.db(MONGO_TEST_DB)
      @grid = Mongo::GridFileSystem.new(@db)
      @filename = 'sample'
    end

    teardown do
      @grid.delete(@filename)
    end

    should "should acknowledge writes by default using md5" do
      file = @grid.open(@filename, 'w')
      file.write "Hello world!"
      file.close
      assert_equal file.client_md5, file.server_md5
    end

    should "should allow for unacknowledged writes" do
      file = @grid.open(@filename, 'w', {:w => 0} )
      file.write "Hello world!"
      file.close
      assert_nil file.client_md5, file.server_md5
    end

    should "should support legacy write concern api" do
      file = @grid.open(@filename, 'w', {:safe => false} )
      file.write "Hello world!"
      file.close
      assert_nil file.client_md5, file.server_md5
    end

  end

end
