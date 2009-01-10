HERE = File.dirname(__FILE__)
$LOAD_PATH[0,0] = File.join(HERE, '..', 'lib')
require 'mongo'
require 'mongo/util/xml_to_ruby'
require 'test/unit'

# For each xml/bson file in the data subdirectory, we turn the XML into an
# OrderedHash and then test both Ruby-to-BSON and BSON-to-Ruby translations.
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

#   # Round-trip comparisons of Ruby-to-BSON and back.
#   # * Take the objects that were read from XML
#   # * Turn them into BSON bytes
#   # * Compare that with the BSON files we have
#   # * Turn those BSON bytes back in to Ruby objects
#   # * Turn them back into BSON bytes
#   # * Compare that with the BSON files we have (or the bytes that were already
#   #   generated)
#   def test_round_trip
#     @@ruby.each { |name, obj|
#       File.open(File.join(HERE, 'data', "#{name}.bson"), 'r') { |f|
#         # Read the BSON from the file
#         bson = f.read
#         bson = if RUBY_VERSION >= '1.9'
#                  bson.bytes.to_a
#                else
#                  bson.split(//).collect { |c| c[0] }
#                end

#         # Turn the Ruby object into BSON bytes and compare with the BSON bytes
#         # from the file.
#         bson_from_ruby = BSON.new.serialize(obj).to_a

# #         # DEBUG
# #         File.open(File.join(HERE, 'data', "#{name}_out.bson"), 'wb') { |f|
# #           bson_from_ruby.each { |b| f.putc(b) }
# #         }

#         begin
#           assert_equal bson.length, bson_from_ruby.length
#           assert_equal bson, bson_from_ruby
#         rescue => ex
#           $stderr.puts "failure while round-tripping #{name}" # DEBUG
#           raise ex
#         end

#         # Turn those BSON bytes back into a Ruby object.
#         #
#         # We're passing a nil db to the contructor here, but that's OK because
#         # the BSON bytes don't contain the db object in any case.
#         obj_from_bson = BSON.new(nil).deserialize(ByteBuffer.new(bson_from_ruby))
#         assert_kind_of OrderedHash, obj_from_bson

#         # Turn that Ruby object into BSON and compare it to the original BSON
#         # bytes.
#         bson_from_ruby = BSON.new.serialize(obj_from_bson).to_a
#         assert_equal bson.length, bson_from_ruby.length
#         assert_equal bson, bson_from_ruby
#       }
#     }
#   end

end
