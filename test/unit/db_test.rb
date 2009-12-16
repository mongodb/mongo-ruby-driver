require 'test/test_helper'

class DBTest < Test::Unit::TestCase

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
      @conn = stub()
      @db   = DB.new("testing", @conn)
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
      @cursor = mock(:next_document => {"ok" => 1})
      Cursor.expects(:new).with(@collection, :admin => true,
        :limit => -1, :selector => {:buildinfo => 1}, :socket => nil).returns(@cursor)
      command = {:buildinfo => 1}
      @db.command(command, true)
    end

    should "raise an error when the command fails" do
      @cursor = mock(:next_document => {"ok" => 0})
      Cursor.expects(:new).with(@collection, :admin => true,
        :limit => -1, :selector => {:buildinfo => 1}, :socket => nil).returns(@cursor)
      assert_raise OperationFailure do
        command = {:buildinfo => 1}
        @db.command(command, true, true)
      end
    end
  end
end


