# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# NOTE: assumes Mongo is running
class TestCollection < Test::Unit::TestCase
  include XGen::Mongo
  include XGen::Mongo::Driver

  @@db = Mongo.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                   ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT).db('ruby-mongo-test')
  @@test = @@db.collection("test")

  def setup
    @@test.drop()
  end

  def test_safe_insert
    a = {"hello" => "world"}
    @@test.insert(a)
    a = @@test.find_one() # TODO we need this because insert doesn't add _id
    @@test.insert(a)
    assert @@db.error.include? "E11000"

    assert_raise OperationFailure do
      @@test.insert(a, :safe => true)
    end
  end

  def test_update
    id1 = @@test.save("x" => 5)
    @@test.update({}, {"$inc" => {"x" => 1}})
    assert_equal 1, @@test.count()
    assert_equal 6, @@test.find_one(:_id => id1)["x"]

    id2 = @@test.save("x" => 1)
    @@test.update({"x" => 6}, {"$inc" => {"x" => 1}})
    assert_equal 7, @@test.find_one(:_id => id1)["x"]
    assert_equal 1, @@test.find_one(:_id => id2)["x"]
  end

  def test_upsert
    @@test.update({"page" => "/"}, {"$inc" => {"count" => 1}}, :upsert => true)
    @@test.update({"page" => "/"}, {"$inc" => {"count" => 1}}, :upsert => true)

    assert_equal 1, @@test.count()
    assert_equal 2, @@test.find_one()["count"]
  end

  def test_safe_update
    @@test.create_index("x")
    @@test.insert("x" => 5)

    @@test.update({}, {"$inc" => {"x" => 1}})
    assert @@db.error?

    assert_raise OperationFailure do
      @@test.update({}, {"$inc" => {"x" => 1}}, :safe => true)
    end
  end

  def test_safe_save
    @@test.create_index("hello", true)

    @@test.save("hello" => "world")
    @@test.save("hello" => "world")
    assert @@db.error.include? "E11000"

    assert_raise OperationFailure do
      @@test.save({"hello" => "world"}, :safe => true)
    end
  end

  def test_find_one
    id = @@test.save("hello" => "world")

    assert_equal "world", @@test.find_one()["hello"]
    assert_equal @@test.find_one(id), @@test.find_one()
    assert_equal @@test.find_one(nil), @@test.find_one()
    assert_equal @@test.find_one({}), @@test.find_one()
    assert_equal @@test.find_one("hello" => "world"), @@test.find_one()
    assert_equal @@test.find_one(OrderedHash["hello", "world"]), @@test.find_one()

    assert_equal nil, @@test.find_one("hello" => "foo")
    assert_equal nil, @@test.find_one(OrderedHash["hello", "foo"])
    assert_equal nil, @@test.find_one(ObjectID.new)

    assert_raise TypeError do
      @@test.find_one(6)
    end
  end
end

