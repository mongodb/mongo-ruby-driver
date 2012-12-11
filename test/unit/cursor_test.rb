require 'test_helper'

class CursorTest < Test::Unit::TestCase
  class Mongo::Cursor
    public :construct_query_spec
  end

  context "Cursor options" do
    setup do
      @logger     = mock()
      @logger.stubs(:debug)
      @connection = stub(:class => MongoClient, :logger => @logger,
        :slave_ok? => false, :read => :primary, :log_duration => false,
        :tag_sets => [], :acceptable_latency => 10)
      @db         = stub(:name => "testing", :slave_ok? => false,
        :connection => @connection, :read => :primary,
        :tag_sets => [], :acceptable_latency => 10)
      @collection = stub(:db => @db, :name => "items", :read => :primary,
        :tag_sets => [], :acceptable_latency => 10)
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

    context "conected to mongos" do
      setup do
        @connection.stubs(:mongos?).returns(true)
        @tag_sets = [{:dc => "ny"}]
      end

      should "set $readPreference" do
        # secondary
        cursor = Cursor.new(@collection, { :read => :secondary })

        spec = cursor.construct_query_spec
        assert spec.has_key?('$readPreference')
        assert_equal :secondary, spec['$readPreference'][:mode]
        assert !spec['$readPreference'].has_key?(:tags)

        # secondary preferred with tags
        cursor = Cursor.new(@collection, { :read => :secondary_preferred, :tag_sets => @tag_sets })

        spec = cursor.construct_query_spec
        assert spec.has_key?('$readPreference')
        assert_equal :secondaryPreferred, spec['$readPreference'][:mode]
        assert_equal @tag_sets, spec['$readPreference'][:tags]

        # primary preferred
        cursor = Cursor.new(@collection, { :read => :primary_preferred })

        spec = cursor.construct_query_spec
        assert spec.has_key?('$readPreference')
        assert_equal :primaryPreferred, spec['$readPreference'][:mode]
        assert !spec['$readPreference'].has_key?(:tags)

        # primary preferred with tags
        cursor = Cursor.new(@collection, { :read => :primary_preferred, :tag_sets => @tag_sets })

        spec = cursor.construct_query_spec
        assert spec.has_key?('$readPreference')
        assert_equal :primaryPreferred, spec['$readPreference'][:mode]
        assert_equal @tag_sets, spec['$readPreference'][:tags]

        # nearest
        cursor = Cursor.new(@collection, { :read => :nearest })

        spec = cursor.construct_query_spec
        assert spec.has_key?('$readPreference')
        assert_equal :nearest, spec['$readPreference'][:mode]
        assert !spec['$readPreference'].has_key?(:tags)

        # nearest with tags
        cursor = Cursor.new(@collection, { :read => :nearest, :tag_sets => @tag_sets })

        spec = cursor.construct_query_spec
        assert spec.has_key?('$readPreference')
        assert_equal :nearest, spec['$readPreference'][:mode]
        assert_equal @tag_sets, spec['$readPreference'][:tags]
      end

      should "not set $readPreference" do
        # for primary
        cursor = Cursor.new(@collection, { :read => :primary, :tag_sets => @tag_sets })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        # for secondary_preferred with no tags
        cursor = Cursor.new(@collection, { :read => :secondary_preferred })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        cursor = Cursor.new(@collection, { :read => :secondary_preferred, :tag_sets => [] })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        cursor = Cursor.new(@collection, { :read => :secondary_preferred, :tag_sets => nil })
        assert !cursor.construct_query_spec.has_key?('$readPreference')
      end
    end

    context "not conected to mongos" do
      setup do
        @connection.stubs(:mongos?).returns(false)
      end

      should "not set $readPreference" do
        cursor = Cursor.new(@collection, { :read => :primary })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        cursor = Cursor.new(@collection, { :read => :primary_preferred })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        cursor = Cursor.new(@collection, { :read => :secondary })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        cursor = Cursor.new(@collection, { :read => :secondary_preferred })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        cursor = Cursor.new(@collection, { :read => :nearest })
        assert !cursor.construct_query_spec.has_key?('$readPreference')

        cursor = Cursor.new(@collection, { :read => :secondary , :tag_sets => @tag_sets})
        assert !cursor.construct_query_spec.has_key?('$readPreference')
      end
    end
  end

  context "Query fields" do
    setup do
      @logger     = mock()
      @logger.stubs(:debug)
      @connection = stub(:class => MongoClient, :logger => @logger, :slave_ok? => false,
        :log_duration => false, :tag_sets =>{}, :acceptable_latency => 10)
      @db = stub(:slave_ok? => true, :name => "testing", :connection => @connection,
        :tag_sets => {}, :acceptable_latency => 10)
      @collection = stub(:db => @db, :name => "items", :read => :primary,
        :tag_sets => {}, :acceptable_latency => 10)
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

  context "counts" do
    setup do
      @logger     = mock()
      @logger.stubs(:debug)
      @connection = stub(:class => Connection, :logger => @logger,
        :slave_ok? => false, :read => :primary, :log_duration => false,
        :tag_sets => {}, :acceptable_latency => 10)
      @db         = stub(:name => "testing", :slave_ok? => false,
        :connection => @connection, :read => :primary,
        :tag_sets => {}, :acceptable_latency => 10)
      @collection = stub(:db => @db, :name => "items", :read => :primary,
        :tag_sets => {}, :acceptable_latency => 10)
      @cursor     = Cursor.new(@collection)
    end

    should "pass the comment parameter" do
      query = {:field => 7}
      @db.expects(:command).with({ 'count' => "items",
                                   'query' => query,
                                   'fields' => nil},
                                 { :read => :primary,
                                   :comment => "my comment"}).
        returns({'ok' => 1, 'n' => 1})
      assert_equal(1, Cursor.new(@collection, :selector => query, :comment => 'my comment').count())
    end
  end
end
