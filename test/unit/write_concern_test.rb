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

class WriteConcernUnitTest < Test::Unit::TestCase

  context "Write-Concern modes on Mongo::MongoClient " do
    setup do
      @write_concern = {
        :w        => 7,
        :j        => false,
        :fsync    => false,
        :wtimeout => nil
      }

      class Mongo::MongoClient
        public :build_get_last_error_message, :build_command_message
      end

      @client =
        MongoClient.new('localhost', 27017,
          @write_concern.merge({:connect => false}))
    end

    should "propogate to DB" do
      db = @client[TEST_DB]
      assert_equal @write_concern, db.write_concern


      db = @client.db(TEST_DB)
      assert_equal @write_concern, db.write_concern

      db = DB.new(TEST_DB, @client)
      assert_equal @write_concern, db.write_concern
    end

    should "allow db override" do
      db = DB.new(TEST_DB, @client, :w => 0)
      assert_equal 0, db.write_concern[:w]

      db = @client.db(TEST_DB, :w => 0)
      assert_equal 0, db.write_concern[:w]
    end

    context "on DB: " do
      setup do
        @db = @client[TEST_DB]
      end

      should "propogate to collection" do
        collection = @db.collection('bar')
        assert_equal @write_concern, collection.write_concern

        collection = @db['bar']
        assert_equal @write_concern, collection.write_concern

        collection = Collection.new('bar', @db)
        assert_equal @write_concern, collection.write_concern
      end

      should "allow override on collection" do
        collection = @db.collection('bar', :w => 0)
        assert_equal 0, collection.write_concern[:w]

        collection = Collection.new('bar', @db, :w => 0)
        assert_equal 0, collection.write_concern[:w]
      end
    end

    context "on operations supporting 'gle' mode" do
      setup do
        @collection = @client[TEST_DB]['bar']
      end

      should "not send w = 1 to the server" do
        gle = @client.build_get_last_error_message("fake", {:w => 1})
        assert_equal gle, @client.build_command_message("fake", {:getlasterror => 1})
      end

      should "use default value on insert" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == @write_concern
        end

        @collection.insert({:a => 1})
      end

      should "allow override alternate value on insert" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == {:w => 100, :j => false, :fsync => false, :wtimeout => nil}
        end

        @collection.insert({:a => 1}, {:w => 100})
      end

      should "allow override to disable on insert" do
        @client.expects(:send_message)
        @collection.insert({:a => 1}, :w => 0)
      end

      should "use default value on update" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == @write_concern
        end

        @collection.update({:a => 1}, {:a => 2})
      end

      should "allow override alternate value on update" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == {:w => 100, :j => false, :fsync => false, :wtimeout => nil}
        end

        @collection.update({:a => 1}, {:a => 2}, {:w => 100})
      end

      should "allow override to disable on update" do
        @client.expects(:send_message)
        @collection.update({:a => 1}, {:a => 2}, :w => 0)
      end

      should "use default value on save" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == @write_concern
        end
        @collection.save({:a => 1})
      end

      should "allow override alternate value on save" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == @write_concern.merge(:w => 1)
        end
        @collection.save({:a => 1}, :w => 1)
      end

      should "allow override to disable on save" do
        @client.expects(:send_message)
        @collection.save({:a => 1}, :w => 0)
      end

      should "use default value on remove" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == @write_concern
        end

        @collection.remove
      end

      should "allow override alternate value on remove" do
        @client.expects(:send_message_with_gle).with do |op, msg, log, n, wc|
          wc == {:w => 100, :j => false, :fsync => false, :wtimeout => nil}
        end

        @collection.remove({}, {:w => 100})
      end

      should "allow override to disable on remove" do
        @client.expects(:send_message)
        @collection.remove({}, :w => 0)
      end
    end
  end
end
