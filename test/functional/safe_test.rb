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

class SafeTest < Test::Unit::TestCase
  context "Safe mode propogation: " do
    setup do
      @connection = standard_connection({:safe => true}, true) # Legacy
      @db         = @connection[MONGO_TEST_DB]
      @collection = @db['test-safe']
      @collection.create_index([[:a, 1]], :unique => true)
      @collection.remove
    end

    should "propogate safe option on insert" do
      @collection.insert({:a => 1})

      assert_raise_error(OperationFailure, "duplicate key") do
        @collection.insert({:a => 1})
      end
    end

    should "allow safe override on insert" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 1}, :safe => false)
    end

    should "allow safe override on save" do
      @collection.insert({:a => 1})
      id = @collection.insert({:a => 2})
      assert_nothing_raised do
        @collection.save({:_id => id.to_s, :a => 1}, :safe => false)
      end
    end

    should "propogate safe option on save" do
      @collection.insert({:a => 1})
      id = @collection.insert({:a => 2})
      assert_raise(OperationFailure) do
        @collection.save({:_id => id.to_s, :a => 1})
      end
    end

    should "propogate safe option on update" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 2})

      assert_raise_error(OperationFailure, "duplicate key") do
        @collection.update({:a => 2}, {:a => 1})
      end
    end

    should "allow safe override on update" do
      @collection.insert({:a => 1})
      @collection.insert({:a => 2})
      @collection.update({:a => 2}, {:a => 1}, :safe => false)
    end
  end

  context "Safe error objects" do
    setup do
      @connection = standard_connection({:safe => true}, true) # Legacy
      @db         = @connection[MONGO_TEST_DB]
      @collection = @db['test']
      @collection.remove
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
      @collection.insert({:a => 1})
    end

    should "return object on update" do
      response = @collection.update({:a => 1}, {"$set" => {:a => 2}},
                             :multi => true)

      assert response['updatedExisting']
      assert_equal 3, response['n']
    end

    should "return object on remove" do
      response = @collection.remove({})
      assert_equal 3, response['n']
    end
  end

end