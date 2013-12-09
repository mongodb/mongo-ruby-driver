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

module Mongo
  class Collection
    attr_reader :operation_writer,
                :command_writer
  end
end

class CollectionUnitTest < Test::Unit::TestCase

  context "Basic operations: " do
    setup do
      @logger = mock()
      @logger.stubs(:level => 0)
      @logger.expects(:debug)

      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db     = @client[TEST_DB]
      @coll   = @db.collection('collection-unit-test')
    end

    should "send update message" do
      @client.expects(:send_message_with_gle).with do |op, msg, log|
        op == 2001
      end
      @coll.operation_writer.stubs(:log_operation)
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send insert message" do
      @client.expects(:send_message_with_gle).with do |op, msg, log|
        op == 2002
      end
      @coll.operation_writer.expects(:log_operation).with do |name, payload|
        (name == :insert) && payload[:documents][:title].include?('Moby')
      end
      @coll.insert({:title => 'Moby Dick'})
    end

    should "send sort data" do
      @client.expects(:checkout_reader).returns(new_mock_socket)
      @client.expects(:receive_message).with do |op, msg, log, sock|
        op == 2004
      end.returns([[], 0, 0])
      @logger.expects(:debug)
      @coll.find({:title => 'Moby Dick'}).sort([['title', 1], ['author', 1]]).next_document
    end

    should "not log binary data" do
      data = BSON::Binary.new(("BINARY " * 1000).unpack("c*"))
      @client.expects(:send_message_with_gle).with do |op, msg, log|
        op == 2002
      end
      @coll.operation_writer.expects(:log_operation).with do |name, payload|
        (name == :insert) && payload[:documents][:data].inspect.include?('Binary')
      end
      @coll.insert({:data => data})
    end

    should "send safe update message" do
      @client.expects(:send_message_with_gle).with do |op, msg, db_name, log|
        op == 2001
      end
      @coll.operation_writer.expects(:log_operation).with do |name, payload|
        (name == :update) && payload[:documents][:title].include?('Moby')
      end
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send safe update message with legacy" do
      connection = Connection.new('localhost', 27017, :safe => true, :connect => false)
      db         = connection[TEST_DB]
      coll       = db.collection('collection-unit-test')
      connection.expects(:send_message_with_gle).with do |op, msg, db_name, log|
        op == 2001
      end
      coll.operation_writer.expects(:log_operation).with do |name, payload|
        (name == :update) && payload[:documents][:title].include?('Moby')
      end
      coll.update({}, {:title => 'Moby Dick'})
    end

    should "send safe insert message" do
      @client.expects(:send_message_with_gle).with do |op, msg, db_name, log|
        op == 2001
      end
      @coll.operation_writer.stubs(:log_operation)
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "not call insert for each ensure_index call" do
      @coll.expects(:generate_indexes).once

      @coll.ensure_index [["x", Mongo::DESCENDING]]
      @coll.ensure_index [["x", Mongo::DESCENDING]]
    end

    should "call generate_indexes for a new type on the same field for ensure_index" do
      @coll.expects(:generate_indexes).twice

      @coll.ensure_index [["x", Mongo::DESCENDING]]
      @coll.ensure_index [["x", Mongo::ASCENDING]]
    end

    should "call generate_indexes twice because the cache time is 0 seconds" do
      @db.cache_time = 0
      @coll = @db.collection('collection-unit-test')
      @coll.expects(:generate_indexes).twice

      @coll.ensure_index [["x", Mongo::DESCENDING]]
      @coll.ensure_index [["x", Mongo::DESCENDING]]
    end

    should "call generate_indexes for each key when calling ensure_indexes" do
      @db.cache_time = 300
      @coll = @db.collection('collection-unit-test')
      @coll.expects(:generate_indexes).once.with do |a, b, c|
        a == {"x"=>-1, "y"=>-1}
      end

      @coll.ensure_index [["x", Mongo::DESCENDING], ["y", Mongo::DESCENDING]]
    end

    should "call generate_indexes for each key when calling ensure_indexes with a hash" do
      @db.cache_time = 300
      @coll = @db.collection('collection-unit-test')
      oh = BSON::OrderedHash.new
      oh['x'] = Mongo::DESCENDING
      oh['y'] = Mongo::DESCENDING
      @coll.expects(:generate_indexes).once.with do |a, b, c|
        a == oh
      end

      if RUBY_VERSION > '1.9'
          @coll.ensure_index({"x" => Mongo::DESCENDING, "y" => Mongo::DESCENDING})
      else
          ordered_hash = BSON::OrderedHash.new
          ordered_hash['x'] = Mongo::DESCENDING
          ordered_hash['y'] = Mongo::DESCENDING
          @coll.ensure_index(ordered_hash)
      end
    end

    should "use the connection's logger" do
      @logger.expects(:warn).with do |msg|
        msg == "MONGODB [WARNING] test warning"
      end
      @coll.log(:warn, "test warning")
    end
  end
end
