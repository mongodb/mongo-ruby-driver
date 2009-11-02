require 'test/test_helper'

class CollectionTest < Test::Unit::TestCase

  class MockDB < DB
    def connect_to_master
      true
    end
  end

  context "Basic operations: " do 
    setup do 
      @logger = mock()
    end

    should "send update message" do 
      @db = MockDB.new("testing", ['localhost', 27017], :logger => @logger)
      @coll = @db.collection('books')
      @db.expects(:send_message_with_operation).with do |op, msg, log| 
        op == 2001 && log.include?("db.books.update")
      end
      @coll.update({}, {:title => 'Moby Dick'})
    end

    should "send insert message" do 
      @db = MockDB.new("testing", ['localhost', 27017], :logger => @logger)
      @coll = @db.collection('books')
      @db.expects(:send_message_with_operation).with do |op, msg, log| 
        op == 2002 && log.include?("db.books.insert")
      end
      @coll.insert({:title => 'Moby Dick'})
    end
  end
end

 
