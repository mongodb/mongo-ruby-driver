HERE = File.dirname(__FILE__)
$LOAD_PATH[0,0] = File.join(HERE, '..', 'lib')
require 'mongo'
require 'mongo/util/xml_to_ruby'
require 'test/unit'

# For each xml/bson file in the data subdirectory, we turn the XML into an
# OrderedHash and then test both Ruby-to-BSON and BSON-to-Ruby translations.
#
# There is a whole other project that includes similar tests
# (http://github.com/mongodb/mongo-qa). If the directory ../../mongo-qa
# exists, (that is, the top-level dir of mongo-qa is next to the top-level dir
# of this project), then we find the BSON test files there and use those, too.
class RoundTripTest < Test::Unit::TestCase

  include XGen::Mongo::Driver

  @@ruby = nil

  def setup
    unless @@ruby
      names = Dir[File.join(HERE, 'data', '*.xml')].collect {|f| File.basename(f).sub(/\.xml$/, '') }
      @@ruby = {}
      names.each { |name|
        File.open(File.join(HERE, 'data', "#{name}.xml")) { |f|
          @@ruby[name] = XMLToRuby.new.xml_to_ruby(f)
        }
      }
    end
  end

  def test_dummy
    assert true
  end

  def self.create_test_for_round_trip_files_in_dir(dir)
    names = Dir[File.join(dir, '*.xson')].collect {|f| File.basename(f).sub(/\.xson$/, '') }
    names.each { |name|
      eval <<EOS
def test_#{name}_#{dir.gsub(/[^a-zA-Z0-9_]/, '_')}
  one_round_trip("#{dir}", "#{name}")
end
EOS
    }
  end

  # Dynamically generate one test for each test file. This way, if one test
  # fails the others will still run.
  create_test_for_round_trip_files_in_dir(File.join(HERE, 'data'))
  mongo_qa_dir = File.join(HERE, '../..', 'mongo-qa/modules/bson_tests/tests')
  if File.exist?(mongo_qa_dir)
    %w(basic_types complex single_types).each { |subdir_name|
      create_test_for_round_trip_files_in_dir(File.join(mongo_qa_dir, subdir_name))
    }
  end

  # Round-trip comparisons of Ruby-to-BSON and back.
  # * Take the objects that were read from XML
  # * Turn them into BSON bytes
  # * Compare that with the BSON files we have
  # * Turn those BSON bytes back in to Ruby objects
  # * Turn them back into BSON bytes
  # * Compare that with the BSON files we have (or the bytes that were already
  #   generated)
  def one_round_trip(dir, name)
    obj = File.open(File.join(dir, "#{name}.xson")) { |f|
      XMLToRuby.new.xml_to_ruby(f)
    }

    File.open(File.join(dir, "#{name}.bson"), 'rb') { |f|
      # Read the BSON from the file
      bson = f.read
      bson = if RUBY_VERSION >= '1.9'
               bson.bytes.to_a
             else
               bson.split(//).collect { |c| c[0] }
             end

      # Turn the Ruby object into BSON bytes and compare with the BSON bytes
      # from the file.
      bson_from_ruby = BSON.new.serialize(obj).to_a

      begin
        assert_equal bson.length, bson_from_ruby.length
        assert_equal bson, bson_from_ruby
      rescue => ex
#         File.open(File.join(dir, "#{name}_out_a.bson"), 'wb') { |f| # DEBUG
#           bson_from_ruby.each { |b| f.putc(b) }
#         }
        raise ex
      end

      # Turn those BSON bytes back into a Ruby object.
      #
      # We're passing a nil db to the contructor here, but that's OK because
      # the BSON DBRef bytes don't contain the db object in any case, and we
      # don't care what the database is.
      obj_from_bson = BSON.new.deserialize(ByteBuffer.new(bson_from_ruby))
      assert_kind_of OrderedHash, obj_from_bson

      # Turn that Ruby object into BSON and compare it to the original BSON
      # bytes.
      bson_from_ruby = BSON.new.serialize(obj_from_bson).to_a
      begin
        assert_equal bson.length, bson_from_ruby.length
        assert_equal bson, bson_from_ruby
      rescue => ex
#         File.open(File.join(dir, "#{name}_out_b.bson"), 'wb') { |f| # DEBUG
#           bson_from_ruby.each { |b| f.putc(b) }
#         }
        raise ex
      end
    }
  end

end
