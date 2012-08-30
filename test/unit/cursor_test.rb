require File.expand_path("../../test_helper", __FILE__)

class CursorTest < Test::Unit::TestCase
  context "Cursor options" do
    setup do
      @logger     = mock()
      @logger.stubs(:debug)
      @connection = stub(:class => Connection, :logger => @logger,
        :slave_ok? => false, :read_preference => :primary, :log_duration => false)
      @db         = stub(:name => "testing", :slave_ok? => false,
        :connection => @connection, :read_preference => :primary)
      @collection = stub(:db => @db, :name => "items", :read_preference => :primary)
      @cursor     = Cursor.new(@collection)
    end

    should "set timeout" do
      assert @cursor.timeout
      assert @cursor.query_options_hash[:timeout]
    end

    should "set selector" do
      assert_equal({}, @cursor.selector)

      @cursor = Cursor.new(@collection, :selector => {:name => "Jones"})
      assert_equal({:name => "Jones"}, @cursor.selector)
      assert_equal({:name => "Jones"}, @cursor.query_options_hash[:selector])
    end

    should "set fields" do
      assert_nil @cursor.fields

      @cursor = Cursor.new(@collection, :fields => [:name, :date])
      assert_equal({:name => 1, :date => 1}, @cursor.fields)
      assert_equal({:name => 1, :date => 1}, @cursor.query_options_hash[:fields])
    end

    should "set mix fields 0 and 1" do
      assert_nil @cursor.fields

      @cursor = Cursor.new(@collection, :fields => {:name => 1, :date => 0})
      assert_equal({:name => 1, :date => 0}, @cursor.fields)
      assert_equal({:name => 1, :date => 0}, @cursor.query_options_hash[:fields])
    end

    should "set limit" do
      assert_equal 0, @cursor.limit

      @cursor = Cursor.new(@collection, :limit => 10)
      assert_equal 10, @cursor.limit
      assert_equal 10, @cursor.query_options_hash[:limit]
    end


    should "set skip" do
      assert_equal 0, @cursor.skip

      @cursor = Cursor.new(@collection, :skip => 5)
      assert_equal 5, @cursor.skip
      assert_equal 5, @cursor.query_options_hash[:skip]
    end

    should "set sort order" do
      assert_nil @cursor.order

      @cursor = Cursor.new(@collection, :order => "last_name")
      assert_equal "last_name", @cursor.order
      assert_equal "last_name", @cursor.query_options_hash[:order]
    end

    should "set hint" do
      assert_nil @cursor.hint

      @cursor = Cursor.new(@collection, :hint => "name")
      assert_equal "name", @cursor.hint
      assert_equal "name", @cursor.query_options_hash[:hint]
    end

    should "set comment" do
      assert_nil @cursor.comment

      @cursor = Cursor.new(@collection, :comment => "comment")
      assert_equal "comment", @cursor.comment
      assert_equal "comment", @cursor.query_options_hash[:comment]
    end

    should "cache full collection name" do
      assert_equal "testing.items", @cursor.full_collection_name
    end
    
    should "raise error when batch_size is 1" do
      e = assert_raise ArgumentError do
        @cursor.batch_size(1)
        end
        assert_equal "Invalid value for batch_size 1; must be 0 or > 1.", e.message
    end
    
    should "use the limit for batch size when it's smaller than the specified batch_size" do
      @cursor.limit(99)
      @cursor.batch_size(100)
      assert_equal 99, @cursor.batch_size
      end
      
    should "use the specified batch_size" do
      @cursor.batch_size(100)
      assert_equal 100, @cursor.batch_size
    end
  end

  context "Query fields" do
    setup do
      @logger     = mock()
      @logger.stubs(:debug)
      @connection = stub(:class => Connection, :logger => @logger, :slave_ok? => false,
        :log_duration => false)
      @db = stub(:slave_ok? => true, :name => "testing", :connection => @connection)
      @collection = stub(:db => @db, :name => "items", :read_preference => :primary)
    end

    should "when an array should return a hash with each key" do
      @cursor = Cursor.new(@collection, :fields => [:name, :age])
      result  = @cursor.fields
      assert_equal result.keys.sort{|a,b| a.to_s <=> b.to_s}, [:age, :name].sort{|a,b| a.to_s <=> b.to_s}
      assert result.values.all? {|v| v == 1}
    end

    should "when a string, return a hash with just the key" do
      @cursor = Cursor.new(@collection, :fields => "name")
      result  = @cursor.fields
      assert_equal result.keys.sort, ["name"]
      assert result.values.all? {|v| v == 1}
    end

    should "return nil when neither hash nor string nor symbol" do
      @cursor = Cursor.new(@collection, :fields => 1234567)
      assert_nil @cursor.fields
    end
  end
end
