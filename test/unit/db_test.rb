require 'test/test_helper'

class DBTest < Test::Unit::TestCase

  class MockDB < DB
    attr_accessor :socket

    def connect_to_master
      true
    end

    public :add_message_headers
  end

  def insert_message(db, documents)
    documents = [documents] unless documents.is_a?(Array)
    message = ByteBuffer.new
    message.put_int(0)
    BSON.serialize_cstr(message, "#{db.name}.test")
    documents.each { |doc| message.put_array(BSON.new.serialize(doc, true).to_a) }
    message = db.add_message_headers(Mongo::Constants::OP_INSERT, message)
  end

  context "DB commands" do 
    setup do 
      @db = MockDB.new("testing", ['localhost', 27017])
      @collection = mock()
      @db.stubs(:system_command_collection).returns(@collection)
    end

    should "raise an error if given a hash with more than one key" do
      assert_raise MongoArgumentError do 
        @db.command(:buildinfo => 1, :somekey => 1)
      end
    end

    should "raise an error if the selector is omitted" do 
      assert_raise MongoArgumentError do 
        @db.command({}, true)
      end
    end

    should "create the proper cursor" do 
      @cursor = mock(:next_object => {"ok" => 1})
      Cursor.expects(:new).with(@collection, :admin => true,
        :limit => -1, :selector => {:buildinfo => 1}).returns(@cursor)
      command = {:buildinfo => 1}
      @db.command(command, true)
    end

    should "raise an error when the command fails" do 
      @cursor = mock(:next_object => {"ok" => 0})
      Cursor.expects(:new).with(@collection, :admin => true,
        :limit => -1, :selector => {:buildinfo => 1}).returns(@cursor)
      assert_raise OperationFailure do 
        command = {:buildinfo => 1}
        @db.command(command, true, true)
      end
    end
  end

  context "safe messages" do
    setup do
      @db = MockDB.new("testing", ['localhost', 27017])
      @collection = mock()
      @db.stubs(:system_command_collection).returns(@collection)
    end

    should "receive getlasterror message" do
      @socket = mock()
      @socket.stubs(:close)
      @socket.expects(:flush)
      @socket.expects(:print).with { |message| message.include?('getlasterror') }
      @db.socket = @socket
      @db.stubs(:receive)
      message = insert_message(@db, {:a => 1})
      @db.send_message_with_safe_check(Mongo::Constants::OP_QUERY, message)
    end
  end
end

 
