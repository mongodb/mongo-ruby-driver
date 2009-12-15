require 'test/test_helper'
require 'mongo/gridfs'

class GridStoreTest < Test::Unit::TestCase

  include Mongo
  include GridFS

  @@db = Connection.new(ENV['MONGO_RUBY_DRIVER_HOST'] || 'localhost',
                        ENV['MONGO_RUBY_DRIVER_PORT'] || Connection::DEFAULT_PORT).db('ruby-mongo-test')
  @@files = @@db.collection('fs.files')
  @@chunks = @@db.collection('fs.chunks')

  def setup
    @@chunks.remove
    @@files.remove
    GridStore.open(@@db, 'foobar', 'w') { |f| f.write("hello, world!") }
  end

  def teardown
    @@chunks.remove
    @@files.remove
    @@db.error
  end

  def test_exist
    assert GridStore.exist?(@@db, 'foobar')
    assert !GridStore.exist?(@@db, 'does_not_exist')
    assert !GridStore.exist?(@@db, 'foobar', 'another_root')
  end

  def test_list
    assert_equal ['foobar'], GridStore.list(@@db)
    assert_equal ['foobar'], GridStore.list(@@db, 'fs')
    assert_equal [], GridStore.list(@@db, 'my_fs')

    GridStore.open(@@db, 'test', 'w') { |f| f.write("my file") }

    assert_equal ['foobar', 'test'], GridStore.list(@@db)
  end

  def test_small_write
    rows = @@files.find({'filename' => 'foobar'}).to_a
    assert_not_nil rows
    assert_equal 1, rows.length
    row = rows[0]
    assert_not_nil row

    file_id = row['_id']
    assert_kind_of ObjectID, file_id
    rows = @@chunks.find({'files_id' => file_id}).to_a
    assert_not_nil rows
    assert_equal 1, rows.length
  end

  def test_small_file
    rows = @@files.find({'filename' => 'foobar'}).to_a
    assert_not_nil rows
    assert_equal 1, rows.length
    row = rows[0]
    assert_not_nil row
    assert_equal "hello, world!", GridStore.read(@@db, 'foobar')
  end

  def test_overwrite
    GridStore.open(@@db, 'foobar', 'w') { |f| f.write("overwrite") }
    assert_equal "overwrite", GridStore.read(@@db, 'foobar')
  end

  def test_read_length
    assert_equal "hello", GridStore.read(@@db, 'foobar', 5)
  end

  def test_read_with_and_without_length
    GridStore.open(@@db, 'read-types', 'w') do |f|
      f.write('hello, there')
    end

    GridStore.open(@@db, 'read-types', 'r') do |f|
      assert_equal 'hello, ', f.read(7)
      assert_equal 'there', f.read
    end
  end

  def test_access_length
    assert_equal 13, GridStore.new(@@db, 'foobar').length
  end

  # Also tests seek
  def test_read_with_offset
    assert_equal "world!", GridStore.read(@@db, 'foobar', nil, 7)
  end

  def test_seek
    GridStore.open(@@db, 'foobar', 'r') { |f|
      f.seek(0)
      assert_equal 'h', f.getc.chr
      f.seek(7)
      assert_equal 'w', f.getc.chr
      f.seek(4)
      assert_equal 'o', f.getc.chr

      f.seek(-1, IO::SEEK_END)
      assert_equal '!', f.getc.chr
      f.seek(-6, IO::SEEK_END)
      assert_equal 'w', f.getc.chr

      f.seek(0)
      f.seek(7, IO::SEEK_CUR)
      assert_equal 'w', f.getc.chr
      f.seek(-1, IO::SEEK_CUR)
      assert_equal 'w', f.getc.chr
      f.seek(-4, IO::SEEK_CUR)
      assert_equal 'o', f.getc.chr
      f.seek(3, IO::SEEK_CUR)
      assert_equal 'o', f.getc.chr
    }
  end

  def test_multi_chunk
    @@chunks.remove
    @@files.remove

    size = 512
    GridStore.open(@@db, 'biggie', 'w') { |f|
      f.chunk_size = size
      f.write('x' * size)
      f.write('y' * size)
      f.write('z' * size)
    }

    assert_equal 3, @@chunks.count
    #assert_equal ('x' * size) + ('y' * size) + ('z' * size), GridStore.read(@@db, 'biggie')
  end

  def test_puts_and_readlines
    GridStore.open(@@db, 'multiline', 'w') { |f|
      f.puts "line one"
      f.puts "line two\n"
      f.puts "line three"
    }

    lines = GridStore.readlines(@@db, 'multiline')
    assert_equal ["line one\n", "line two\n", "line three\n"], lines
  end

  def test_unlink
    assert_equal 1, @@files.count
    assert_equal 1, @@chunks.count
    GridStore.unlink(@@db, 'foobar')
    assert_equal 0, @@files.count
    assert_equal 0, @@chunks.count
  end

  def test_append
    GridStore.open(@@db, 'foobar', 'w+') { |f| f.write(" how are you?") }
    assert_equal 1, @@chunks.count
    assert_equal "hello, world! how are you?", GridStore.read(@@db, 'foobar')
  end

  def test_rewind_and_truncate_on_write
    GridStore.open(@@db, 'foobar', 'w') { |f|
      f.write("some text is inserted here")
      f.rewind
      f.write("abc")
    }
    assert_equal "abc", GridStore.read(@@db, 'foobar')
  end

  def test_tell
    GridStore.open(@@db, 'foobar', 'r') { |f|
      f.read(5)
      assert_equal 5, f.tell
    }
  end

  def test_empty_block_ok
    GridStore.open(@@db, 'empty', 'w')
  end

  def test_save_empty_file
    @@chunks.remove
    @@files.remove
    GridStore.open(@@db, 'empty', 'w') {} # re-write with zero bytes
    assert_equal 1, @@files.count
    assert_equal 0, @@chunks.count
  end

  def test_empty_file_eof
    GridStore.open(@@db, 'empty', 'w')
    GridStore.open(@@db, 'empty', 'r') { |f|
      assert f.eof?
    }
  end

  def test_cannot_change_chunk_size_on_read
    begin
      GridStore.open(@@db, 'foobar', 'r') { |f| f.chunk_size = 42 }
      fail "should have seen error"
    rescue => ex
      assert_match /error: can only change chunk size/, ex.to_s
    end
  end

  def test_cannot_change_chunk_size_after_data_written
    begin
      GridStore.open(@@db, 'foobar', 'w') { |f|
        f.write("some text")
        f.chunk_size = 42
      }
      fail "should have seen error"
    rescue => ex
      assert_match /error: can only change chunk size/, ex.to_s
    end
  end

  def test_change_chunk_size
    GridStore.open(@@db, 'new-file', 'w') { |f|
      f.chunk_size = 42
      f.write("foo")
    }
    GridStore.open(@@db, 'new-file', 'r') { |f|
      assert f.chunk_size == 42
    }
  end

  def test_chunk_size_in_option
    GridStore.open(@@db, 'new-file', 'w', :chunk_size => 42) { |f| f.write("foo") }
    GridStore.open(@@db, 'new-file', 'r') { |f|
      assert f.chunk_size == 42
    }
  end

  def test_md5
    GridStore.open(@@db, 'new-file', 'w') { |f| f.write("hello world\n")}
    GridStore.open(@@db, 'new-file', 'r') { |f|
      assert f.md5 == '6f5902ac237024bdd0c176cb93063dc4'
      begin
        f.md5 = 'cant do this'
        fail "should have seen error"
      rescue => ex
        true
      end
    }
    GridStore.open(@@db, 'new-file', 'w') {}
    GridStore.open(@@db, 'new-file', 'r') { |f|
      assert f.md5 == 'd41d8cd98f00b204e9800998ecf8427e'
    }
  end

  def test_upload_date
    now = Time.now
    orig_file_upload_date = nil
    GridStore.open(@@db, 'foobar', 'r') { |f| orig_file_upload_date = f.upload_date }
    assert_not_nil orig_file_upload_date
    assert (orig_file_upload_date - now) < 5 # even a really slow system < 5 secs

    sleep(2)
    GridStore.open(@@db, 'foobar', 'w') { |f| f.write "new data" }
    file_upload_date = nil
    GridStore.open(@@db, 'foobar', 'r') { |f| file_upload_date = f.upload_date }
    assert_equal orig_file_upload_date, file_upload_date
  end

  def test_content_type
    ct = nil
    GridStore.open(@@db, 'foobar', 'r') { |f| ct = f.content_type }
    assert_equal GridStore::DEFAULT_CONTENT_TYPE, ct

    GridStore.open(@@db, 'foobar', 'w+') { |f| f.content_type = 'text/html' }
    ct2 = nil
    GridStore.open(@@db, 'foobar', 'r') { |f| ct2 = f.content_type }
    assert_equal 'text/html', ct2
  end

  def test_content_type_option
    GridStore.open(@@db, 'new-file', 'w', :content_type => 'image/jpg') { |f| f.write('foo') }
    ct = nil
    GridStore.open(@@db, 'new-file', 'r') { |f| ct = f.content_type }
    assert_equal 'image/jpg', ct
  end

  def test_unknown_mode
    GridStore.open(@@db, 'foobar', 'x')
    fail 'should have seen "illegal mode" error raised'
  rescue => ex
    assert_equal "error: illegal mode x", ex.to_s
  end

  def test_metadata
    GridStore.open(@@db, 'foobar', 'r') { |f| assert_nil f.metadata }
    GridStore.open(@@db, 'foobar', 'w+') { |f| f.metadata = {'a' => 1} }
    GridStore.open(@@db, 'foobar', 'r') { |f| assert_equal({'a' => 1}, f.metadata) }
  end

end
