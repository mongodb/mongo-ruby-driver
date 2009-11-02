require 'test/test_helper'

class DBTest < Test::Unit::TestCase

  class MockDB < DB

    def connect_to_master
      true
    end
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

end

 
