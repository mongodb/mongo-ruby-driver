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

class TestThreading < Test::Unit::TestCase

  include Mongo

  @@client = standard_connection(:pool_size => 2, :pool_timeout => 30)
  @@db     = @@client[MONGO_TEST_DB]
  @@coll   = @@db.collection('thread-test-collection')

  def set_up_safe_data
    @@db.drop_collection('duplicate')
    @@db.drop_collection('unique')
    @duplicate = @@db.collection('duplicate')
    @unique    = @@db.collection('unique')

    @duplicate.insert("test" => "insert")
    @duplicate.insert("test" => "update")
    @unique.insert("test" => "insert")
    @unique.insert("test" => "update")
    @unique.create_index("test", :unique => true)
  end

  def test_safe_update
    times = []
    set_up_safe_data
    threads = []
    25.times do |i|
      threads[i] = Thread.new do
        100.times do
          if i % 2 == 0
            assert_raise Mongo::OperationFailure do
              t1 = Time.now
              @unique.update({"test" => "insert"}, {"$set" => {"test" => "update"}})
              times << Time.now - t1
            end
          else
            t1 = Time.now
            @duplicate.update({"test" => "insert"}, {"$set" => {"test" => "update"}})
            times << Time.now - t1
          end
        end
      end
    end

    25.times do |i|
      threads[i].join
    end
  end

  def test_safe_insert
    set_up_safe_data
    threads = []
    25.times do |i|
      threads[i] = Thread.new do
        if i % 2 == 0
          assert_raise Mongo::OperationFailure do
            @unique.insert({"test" => "insert"})
          end
        else
          @duplicate.insert({"test" => "insert"})
        end
      end
    end

    25.times do |i|
      threads[i].join
    end
  end

  def test_threading
    @@coll.drop
    @@coll = @@db.collection('thread-test-collection')

    docs = []
    1000.times {|i| docs << {:x => i}}
    @@coll.insert(docs)

    threads = []

    10.times do |i|
      threads[i] = Thread.new do
        sum = 0
        @@coll.find().each do |document|
          sum += document["x"]
        end
        assert_equal 499500, sum
      end
    end

    10.times do |i|
      threads[i].join
    end
  end
end
