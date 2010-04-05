require 'test/test_helper'

class CollectionTest < Test::Unit::TestCase

  context "Basic operations: " do
    setup do
      @logger = mock()
    end

    should "send update message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message).with do |op, msg, log|
        op == 2001 && log.include?("testing['books'].update")
      end
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send insert message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message).with do |op, msg, log|
        op == 2002 && log.include?("testing['books'].insert")
      end
      @coll.insert({:title => 'Moby Dick'})
    end

    should "send sort data" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:receive_message).with do |op, msg, log, sock|
        op == 2004 && log.include?("sort")
      end.returns([[], 0, 0])
      @coll.find({:title => 'Moby Dick'}).sort([['title', 1], ['author', 1]]).next_document
    end

    should "not log binary data" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      data = BSON::Binary.new(("BINARY " * 1000).unpack("c*"))
      @conn.expects(:send_message).with do |op, msg, log|
        op == 2002 && log.include?("BSON::Binary")
      end
      @coll.insert({:data => data})
    end

    should "send safe update message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message_with_safe_check).with do |op, msg, db_name, log|
        op == 2001 && log.include?("testing['books'].update")
      end
      @coll.update({}, {:title => 'Moby Dick'}, :safe => true)
    end

    should "send safe insert message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message_with_safe_check).with do |op, msg, db_name, log|
        op == 2001 && log.include?("testing['books'].update")
      end
      @coll.update({}, {:title => 'Moby Dick'}, :safe => true)
    end
  end
end
