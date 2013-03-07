require 'test_helper'

class CollectionTest < Test::Unit::TestCase

  context "Basic operations: " do
    setup do
      @logger = mock()
      @logger.stubs(:level => 0)
      @logger.expects(:debug)
    end

    should "send update message" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @client.expects(:send_message_with_gle).with do |op, msg, log|
        op == 2001
      end
      @coll.stubs(:log_operation)
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send insert message" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @client.expects(:send_message_with_gle).with do |op, msg, log|
        op == 2002
      end
      @coll.expects(:log_operation).with do |name, payload|
        (name == :insert) && payload[:documents][0][:title].include?('Moby')
      end
      @coll.insert({:title => 'Moby Dick'})
    end

    should "send sort data" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @client.expects(:checkout_reader).returns(new_mock_socket)
      @client.expects(:receive_message).with do |op, msg, log, sock|
        op == 2004
      end.returns([[], 0, 0])
      @logger.expects(:debug)
      @coll.find({:title => 'Moby Dick'}).sort([['title', 1], ['author', 1]]).next_document
    end

    should "not log binary data" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      data = BSON::Binary.new(("BINARY " * 1000).unpack("c*"))
      @client.expects(:send_message_with_gle).with do |op, msg, log|
        op == 2002
      end
      @coll.expects(:log_operation).with do |name, payload|
        (name == :insert) && payload[:documents][0][:data].inspect.include?('Binary')
      end
      @coll.insert({:data => data})
    end

    should "send safe update message" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @client.expects(:send_message_with_gle).with do |op, msg, db_name, log|
        op == 2001
      end
      @coll.expects(:log_operation).with do |name, payload|
        (name == :update) && payload[:document][:title].include?('Moby')
      end
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send safe update message with legacy" do
      @connection = Connection.new('localhost', 27017, :logger => @logger, :safe => true, :connect => false)
      @db         = @connection['testing']
      @coll       = @db.collection('books')
      @connection.expects(:send_message_with_gle).with do |op, msg, db_name, log|
        op == 2001
      end
      @coll.expects(:log_operation).with do |name, payload|
        (name == :update) && payload[:document][:title].include?('Moby')
      end
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send safe insert message" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @client.expects(:send_message_with_gle).with do |op, msg, db_name, log|
        op == 2001
      end
      @coll.stubs(:log_operation)
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "not call insert for each ensure_index call" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @coll.expects(:generate_indexes).once

      @coll.ensure_index [["x", Mongo::DESCENDING]]
      @coll.ensure_index [["x", Mongo::DESCENDING]]
    end

    should "call generate_indexes for a new type on the same field for ensure_index" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @coll.expects(:generate_indexes).twice

      @coll.ensure_index [["x", Mongo::DESCENDING]]
      @coll.ensure_index [["x", Mongo::ASCENDING]]

    end

    should "call generate_indexes twice because the cache time is 0 seconds" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @db.cache_time = 0
      @coll = @db.collection('books')
      @coll.expects(:generate_indexes).twice

      @coll.ensure_index [["x", Mongo::DESCENDING]]
      @coll.ensure_index [["x", Mongo::DESCENDING]]
    end

    should "call generate_indexes for each key when calling ensure_indexes" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @db.cache_time = 300
      @coll = @db.collection('books')
      @coll.expects(:generate_indexes).once.with do |a, b, c|
        a == {"x"=>-1, "y"=>-1}
      end

      @coll.ensure_index [["x", Mongo::DESCENDING], ["y", Mongo::DESCENDING]]
    end

    should "call generate_indexes for each key when calling ensure_indexes with a hash" do
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @db.cache_time = 300
      @coll = @db.collection('books')
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
      @client = MongoClient.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @client['testing']
      @coll = @db.collection('books')
      @logger.expects(:warn).with do |msg|
        msg == "MONGODB [WARNING] test warning"
      end
      @coll.log(:warn, "test warning")
    end
  end
end
