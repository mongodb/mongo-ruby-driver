# Tailable cursors in Ruby

Tailable cursors are cursors that remain open even after they've returned
a final result. This way, if more documents are added to a collection (i.e.,
to the cursor's result set), then you can continue to call `Cursor#next` to
retrieve those results. Here's a complete test case that demonstrates the use
of tailable cursors.

Note that tailable cursors are for capped collections only.

      require 'mongo'
      require 'test/unit'

      class TestTailable < Test::Unit::TestCase
        include Mongo

        def test_tailable

          # Create a connection and capped collection.
          @con = Connection.new
          @db  = @con['test']
          @db.drop_collection('log')
          @capped = @db.create_collection('log', :capped => true, :size => 1024)

          # Insert 10 documents.
          10.times do |n|
            @capped.insert({:n => n})
          end

          # Create a tailable cursor that iterates the collection in natural order
          @tail = Cursor.new(@capped, :tailable => true, :order => [['$natural', 1]])

          # Call Cursor#next 10 times. Each call returns a document.
          10.times do
            assert @tail.next
          end

          # But the 11th time, the cursor returns nothing.
          assert_nil @tail.next

          # Add a document to the capped collection.
          @capped.insert({:n => 100})

          # Now call Cursor#next again. This will return the just-inserted result.
          assert @tail.next

          # Close the cursor.
          @tail.close
        end

      end
