require 'test_helper'

class ReadTest < Test::Unit::TestCase

  context "Read mode on standard connection: " do
    setup do
      @read = :secondary
      @client = MongoClient.new('localhost', 27017, :read => @read, :connect => false)
    end

  end

  context "Read preferences on replica set connection: " do
    setup do
      @read = :secondary_preferred
      @acceptable_latency = 100
      @tags = {"dc" => "Tyler", "rack" => "Brock"}
      @bad_tags = {"wow" => "cool"}
      @client = MongoReplicaSetClient.new(
        ['localhost:27017'],
        :read => @read,
        :tag_sets => @tags,
        :secondary_acceptable_latency_ms => @acceptable_latency,
        :connect => false
      )
    end

    should "store read preference on MongoClient" do
      assert_equal @read, @client.read
      assert_equal @tags, @client.tag_sets
      assert_equal @acceptable_latency, @client.acceptable_latency
    end

    should "propogate to DB" do
      db = @client['foo']
      assert_equal @read, db.read
      assert_equal @tags, db.tag_sets
      assert_equal @acceptable_latency, db.acceptable_latency

      db = @client.db('foo')
      assert_equal @read, db.read
      assert_equal @tags, db.tag_sets
      assert_equal @acceptable_latency, db.acceptable_latency

      db = DB.new('foo', @client)
      assert_equal @read, db.read
      assert_equal @tags, db.tag_sets
      assert_equal @acceptable_latency, db.acceptable_latency
    end

    should "allow db override" do
      db = DB.new('foo', @client, :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
      assert_equal :primary, db.read
      assert_equal @bad_tags, db.tag_sets
      assert_equal 25, db.acceptable_latency

      db = @client.db('foo', :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
      assert_equal :primary, db.read
      assert_equal @bad_tags, db.tag_sets
      assert_equal 25, db.acceptable_latency
    end

    context "on DB: " do
      setup do
        @db = @client['foo']
      end

      should "propogate to collection" do
        col = @db.collection('bar')
        assert_equal @read, col.read
        assert_equal @tags, col.tag_sets
        assert_equal @acceptable_latency, col.acceptable_latency

        col = @db['bar']
        assert_equal @read, col.read
        assert_equal @tags, col.tag_sets
        assert_equal @acceptable_latency, col.acceptable_latency

        col = Collection.new('bar', @db)
        assert_equal @read, col.read
        assert_equal @tags, col.tag_sets
        assert_equal @acceptable_latency, col.acceptable_latency
      end

      should "allow override on collection" do
        col = @db.collection('bar', :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
        assert_equal :primary, col.read
        assert_equal @bad_tags, col.tag_sets
        assert_equal 25, col.acceptable_latency

        col = Collection.new('bar', @db, :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
        assert_equal :primary, col.read
        assert_equal @bad_tags, col.tag_sets
        assert_equal 25, col.acceptable_latency
      end
    end

    context "on read mode ops" do
      setup do
        @col = @client['foo']['bar']
        @mock_socket = new_mock_socket
      end

      should "use default value on query" do
        @cursor = @col.find({:a => 1})
        sock = new_mock_socket
        read_pool = stub(:checkin => true)
        @client.stubs(:read_pool).returns(read_pool)
        local_manager = PoolManager.new(@client, @client.seeds)
        @client.stubs(:local_manager).returns(local_manager)
        primary_pool = stub(:checkin => true)
        sock.stubs(:pool).returns(primary_pool)
        @client.stubs(:primary_pool).returns(primary_pool)
        @client.expects(:checkout_reader).returns(sock)
        @client.expects(:receive_message).with do |o, m, l, s, c, r|
          r == nil
        end.returns([[], 0, 0])

        @cursor.next
      end

      should "allow override default value on query" do
        @cursor = @col.find({:a => 1}, :read => :primary)
        sock = new_mock_socket
        local_manager = PoolManager.new(@client, @client.seeds)
        @client.stubs(:local_manager).returns(local_manager)
        primary_pool = stub(:checkin => true)
        sock.stubs(:pool).returns(primary_pool)
        @client.stubs(:primary_pool).returns(primary_pool)
        @client.expects(:checkout_reader).returns(sock)
        @client.expects(:receive_message).with do |o, m, l, s, c, r|
          r == nil
        end.returns([[], 0, 0])

        @cursor.next
      end

      should "allow override alternate value on query" do
        assert_raise MongoArgumentError do
          @col.find_one({:a => 1}, :read => {:dc => "ny"})
        end
      end
    end
  end
end
