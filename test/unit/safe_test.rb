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

class SafeUnitTest < Test::Unit::TestCase

  context "Write-Concern modes on Mongo::Connection " do
    setup do
      @safe_value = {:w => 7, :j => false, :fsync => false, :wtimeout => nil}
      @connection = Mongo::Connection.new('localhost', 27017, :safe => @safe_value, :connect => false)
    end

    should "propogate to DB" do
      db = @connection[TEST_DB]
      assert_equal @safe_value[:w], db.write_concern[:w]


      db = @connection.db(TEST_DB)
      assert_equal @safe_value[:w], db.write_concern[:w]

      db = DB.new(TEST_DB, @connection)
      assert_equal @safe_value[:w], db.write_concern[:w]
    end

    should "allow db override" do
      db = DB.new(TEST_DB, @connection, :safe => false)
      assert_equal 0, db.write_concern[:w]

      db = @connection.db(TEST_DB, :safe => false)
      assert_equal 0, db.write_concern[:w]
    end

    context "on DB: " do
      setup do
        @db = @connection[TEST_DB]
      end

      should "propogate to collection" do
        col = @db.collection('bar')
        assert_equal @safe_value, col.write_concern

        col = @db['bar']
        assert_equal @safe_value, col.write_concern

        col = Collection.new('bar', @db)
        assert_equal @safe_value, col.write_concern
      end

      should "allow override on collection" do
        col = @db.collection('bar', :safe => false)
        assert_equal 0, col.write_concern[:w]

        col = Collection.new('bar', @db, :safe => false)
        assert_equal 0, col.write_concern[:w]
      end
    end

    context "on operations supporting safe mode" do
      setup do
        @col = @connection[TEST_DB]['bar']
      end

      should "use default value on insert" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == @safe_value
        end

        @col.insert({:a => 1})
      end

      should "allow override alternate value on insert" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == {:w => 100, :j => false, :fsync => false, :wtimeout => nil}
        end

        @col.insert({:a => 1}, :safe => {:w => 100})
      end

      should "allow override to disable on insert" do
        @connection.expects(:send_message)
        @col.insert({:a => 1}, :safe => false)
      end

      should "use default value on update" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == @safe_value
        end

        @col.update({:a => 1}, {:a => 2})
      end

      should "allow override alternate value on update" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == {:w => 100, :j => false, :fsync => false, :wtimeout => nil}
        end

        @col.update({:a => 1}, {:a => 2}, :safe => {:w => 100})
      end

      should "allow override to disable on update" do
        @connection.expects(:send_message)
        @col.update({:a => 1}, {:a => 2}, :safe => false)
      end

      should "use default value on save" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == @safe_value
        end
        @col.save({:a => 1})
      end

      should "allow override alternate value on save" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == @safe_value.merge(:w => 1)
        end
        @col.save({:a => 1}, :safe => true)
      end

      should "allow override to disable on save" do
        @connection.expects(:send_message)
        @col.save({:a => 1}, :safe => false)
      end

      should "use default value on remove" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == @safe_value
        end

        @col.remove
      end

      should "allow override alternate value on remove" do
        @connection.expects(:send_message_with_gle).with do |op, msg, log, n, safe|
          safe == {:w => 100, :j => false, :fsync => false, :wtimeout => nil}
        end

        @col.remove({}, :safe => {:w => 100})
      end

      should "allow override to disable on remove" do
        @connection.expects(:send_message)
        @col.remove({}, :safe => false)
      end
    end
  end
end
