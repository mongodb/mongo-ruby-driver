require 'test/test_helper'

class ConnectionTest < Test::Unit::TestCase

  context "Basic operations: " do
    setup do
      @logger = mock()
    end

    should "send update message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message).with do |op, msg, log|
        op == 2001 && log.include?("db.books.update")
      end
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send insert message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message).with do |op, msg, log|
        op == 2002 && log.include?("db.books.insert")
      end
      @coll.insert({:title => 'Moby Dick'})
    end

    should "send safe update message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message_with_safe_check).with do |op, msg, db_name, log|
        op == 2001 && log.include?("db.books.update")
      end
      @coll.update({}, {:title => 'Moby Dick'}, :safe => true)
    end

    should "send safe insert message" do
      @conn = Connection.new('localhost', 27017, :logger => @logger, :connect => false)
      @db   = @conn['testing']
      @coll = @db.collection('books')
      @conn.expects(:send_message_with_safe_check).with do |op, msg, db_name, log|
        op == 2001 && log.include?("db.books.update")
      end
      @coll.update({}, {:title => 'Moby Dick'}, :safe => true)
    end
  end
end


