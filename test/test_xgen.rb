# Copyright (C) 2009 10gen Inc.
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

$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

# TODO these tests should be removed - just testing for the deprecated
# XGen::Mongo::Driver include path
class TestXGen < Test::Unit::TestCase
  @@db = XGen::Mongo::Driver::Mongo.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                                        ENV['MONGO_RUBY_DRIVER_PORT'] || XGen::Mongo::Driver::Mongo::DEFAULT_PORT).db('ruby-mongo-test')
  @@test = @@db.collection('test')

  def setup
    @@test.clear
  end

  def test_sort
    @@test.save('x' => 2)
    @@test.save('x' => 1)
    @@test.save('x' => 3)

    assert_equal 1, @@test.find({}, :sort => {'x' => XGen::Mongo::ASCENDING}).to_a()[0]['x']
    assert_equal 3, @@test.find({}, :sort => {'x' => XGen::Mongo::DESCENDING}).to_a()[0]['x']
  end

  def test_gridfs
    XGen::Mongo::GridFS::GridStore.open(@@db, 'foobar', 'w') { |f| f.write('hello world!') }
    assert XGen::Mongo::GridFS::GridStore.exist?(@@db, 'foobar')
    assert !XGen::Mongo::GridFS::GridStore.exist?(@@db, 'mike')
  end
end

class TestXGenInclude < Test::Unit::TestCase
  include XGen::Mongo::GridFS
  include XGen::Mongo::Driver
  include XGen::Mongo

  @@db = Mongo.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                   ENV['MONGO_RUBY_DRIVER_PORT'] || Mongo::DEFAULT_PORT).db('ruby-mongo-test')
  @@test = @@db.collection('test')

  def setup
    @@test.clear
  end

  def test_sort
    @@test.save('x' => 2)
    @@test.save('x' => 1)
    @@test.save('x' => 3)

    assert_equal 1, @@test.find({}, :sort => {'x' => ASCENDING}).to_a()[0]['x']
    assert_equal 3, @@test.find({}, :sort => {'x' => DESCENDING}).to_a()[0]['x']
  end

  def test_gridfs
    GridStore.open(@@db, 'foobar', 'w') { |f| f.write('hello world!') }
    assert GridStore.exist?(@@db, 'foobar')
    assert !GridStore.exist?(@@db, 'mike')
  end
end
