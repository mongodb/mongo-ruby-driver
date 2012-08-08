require File.expand_path("../../test_helper", __FILE__)

class ReadTest < Test::Unit::TestCase

  context "Read mode on standard connection: " do
    setup do
      @read_preference = :secondary
      @con = Mongo::Connection.new('localhost', 27017, :read => @read_preference, :connect => false)
    end

  end

  context "Read preferences on replica set connection: " do
    setup do
      @read_preference = :secondary_preferred
      @acceptable_latency = 100
      @tags = {"dc" => "Tyler", "rack" => "Brock"}
      @bad_tags = {"wow" => "cool"}
      @con = Mongo::ReplSetConnection.new(
        ['localhost:27017'],
        :read => @read_preference,
        :tag_sets => @tags,
        :secondary_acceptable_latency_ms => @acceptable_latency,
        :connect => false
      )
    end

    should "store read preference on Connection" do
      assert_equal @read_preference, @con.read_preference
      assert_equal @tags, @con.tag_sets
      assert_equal @acceptable_latency, @con.acceptable_latency
    end

    should "propogate to DB" do
      db = @con['foo']
      assert_equal @read_preference, db.read_preference
      assert_equal @tags, db.tag_sets
      assert_equal @acceptable_latency, db.acceptable_latency

      db = @con.db('foo')
      assert_equal @read_preference, db.read_preference
      assert_equal @tags, db.tag_sets
      assert_equal @acceptable_latency, db.acceptable_latency

      db = DB.new('foo', @con)
      assert_equal @read_preference, db.read_preference
      assert_equal @tags, db.tag_sets
      assert_equal @acceptable_latency, db.acceptable_latency
    end

    should "allow db override" do
      db = DB.new('foo', @con, :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
      assert_equal :primary, db.read_preference
      assert_equal @bad_tags, db.tag_sets
      assert_equal 25, db.acceptable_latency

      db = @con.db('foo', :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
      assert_equal :primary, db.read_preference
      assert_equal @bad_tags, db.tag_sets
      assert_equal 25, db.acceptable_latency
    end

    context "on DB: " do
      setup do
        @db = @con['foo']
      end

      should "propogate to collection" do
        col = @db.collection('bar')
        assert_equal @read_preference, col.read_preference
        assert_equal @tags, col.tag_sets
        assert_equal @acceptable_latency, col.acceptable_latency

        col = @db['bar']
        assert_equal @read_preference, col.read_preference
        assert_equal @tags, col.tag_sets
        assert_equal @acceptable_latency, col.acceptable_latency

        col = Collection.new('bar', @db)
        assert_equal @read_preference, col.read_preference
        assert_equal @tags, col.tag_sets
        assert_equal @acceptable_latency, col.acceptable_latency
      end

      should "allow override on collection" do
        col = @db.collection('bar', :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
        assert_equal :primary, col.read_preference
        assert_equal @bad_tags, col.tag_sets
        assert_equal 25, col.acceptable_latency

        col = Collection.new('bar', @db, :read => :primary, :tag_sets => @bad_tags, :acceptable_latency => 25)
        assert_equal :primary, col.read_preference
        assert_equal @bad_tags, col.tag_sets
        assert_equal 25, col.acceptable_latency
      end
    end

    context "on read mode ops" do
      setup do
        @col = @con['foo']['bar']
        @mock_socket = new_mock_socket
      end

      should "use default value on query" do
        @cursor = @col.find({:a => 1})
        sock = new_mock_socket
        read_pool = stub(:checkin => true)
        @con.stubs(:read_pool).returns(read_pool)
        primary_pool = stub(:checkin => true)
        sock.stubs(:pool).returns(primary_pool)
        @con.stubs(:primary_pool).returns(primary_pool)
        @con.expects(:checkout_reader).returns(sock)
        @con.expects(:receive_message).with do |o, m, l, s, c, r|
          r == nil
        end.returns([[], 0, 0])

        @cursor.next
      end

      should "allow override default value on query" do
        @cursor = @col.find({:a => 1}, :read => :primary)
        sock = new_mock_socket
        primary_pool = stub(:checkin => true)
        sock.stubs(:pool).returns(primary_pool)
        @con.stubs(:primary_pool).returns(primary_pool)
        @con.expects(:checkout_reader).returns(sock)
        @con.expects(:receive_message).with do |o, m, l, s, c, r|
          r == nil
        end.returns([[], 0, 0])

        @cursor.next
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
