require './test/test_helper'

class ReadTest < Test::Unit::TestCase

  context "Read mode on standard connection: " do
    setup do
      @read_preference = :secondary
      @con = Mongo::Connection.new('localhost', 27017, :read => @read_preference, :connect => false)
    end

  end

  context "Read mode on connection: " do
    setup do
      @read_preference = :secondary
      @con = Mongo::ReplSetConnection.new(['localhost', 27017], :read => @read_preference, :connect => false)
    end

    should "store read preference on Connection" do
      assert_equal @read_preference, @con.read_preference
    end

    should "propogate to DB" do
      db = @con['foo']
      assert_equal @read_preference, db.read_preference

      db = @con.db('foo')
      assert_equal @read_preference, db.read_preference

      db = DB.new('foo', @con)
      assert_equal @read_preference, db.read_preference
    end

    should "allow db override" do
      db = DB.new('foo', @con, :read => :primary)
      assert_equal :primary, db.read_preference

      db = @con.db('foo', :read => :primary)
      assert_equal :primary, db.read_preference
    end

    context "on DB: " do
      setup do
        @db = @con['foo']
      end

      should "propogate to collection" do
        col = @db.collection('bar')
        assert_equal @read_preference, col.read_preference

        col = @db['bar']
        assert_equal @read_preference, col.read_preference

        col = Collection.new('bar', @db)
        assert_equal @read_preference, col.read_preference
      end

      should "allow override on collection" do
        col = @db.collection('bar', :read => :primary)
        assert_equal :primary, col.read_preference

        col = Collection.new('bar', @db, :read => :primary)
        assert_equal :primary, col.read_preference
      end
    end

    context "on read mode ops" do
      setup do
        @col = @con['foo']['bar']
        @mock_socket = stub()
      end

      should "use default value on query" do
        @con.expects(:receive_message).with do |o, m, l, s, c, r|
          r == :secondary
        end.returns([[], 0, 0])

        @col.find_one({:a => 1})
      end

      should "allow override default value on query" do
        @con.expects(:receive_message).with do |o, m, l, s, c, r|
          r == :primary
        end.returns([[], 0, 0])

        @col.find_one({:a => 1}, :read => :primary)
      end

      should "allow override alternate value on query" do
        # TODO: enable this test once we enable reading from tags.
        # @con.expects(:receive_message).with do |o, m, l, s, c, r|
        #   tags = {:dc => "ny"}
        # end.returns([[], 0, 0])

        assert_raise MongoArgumentError do
          @col.find_one({:a => 1}, :read => {:dc => "ny"})
        end
      end
    end
  end
end
