HERE = File.dirname(__FILE__)
$LOAD_PATH[0,0] = File.join(HERE, '..', 'lib')
require 'mongo'
require 'rexml/document'
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
      names.each { |name| @@ruby[name] = xml_to_ruby(name) }
    end
  end

  def xml_to_ruby(name)
    File.open(File.join(HERE, 'data', "#{name}.xml")) { |f|
      doc = REXML::Document.new(f)
      doc_to_ruby(doc.root.elements['doc'])
    }
  end

  def element_to_ruby(e)
    type = e.name
    child = e.elements[1]
    case type
    when 'oid'
      ObjectID.from_string(e.text)
    when 'ref'
      dbref_to_ruby(e.elements)
    when 'int'
      e.text.to_i
    when 'number'
      e.text.to_f
    when 'string', 'code'
      e.text.to_s
    when 'boolean'
      e.text.to_s == 'true'
    when 'array'
      array_to_ruby(e.elements)
    when 'date'
      Time.at(e.text.to_f / 1000.0)
    when 'regex'
      regex_to_ruby(e.elements)
    when 'null'
      nil
    when 'doc'
      doc_to_ruby(e)
    else
      raise "Unknown type #{type} in element with name #{e.attributes['name']}"
    end
  end

  def doc_to_ruby(element)
    oh = OrderedHash.new
    element.elements.each { |e| oh[e.attributes['name']] = element_to_ruby(e) }
    oh
  end

  def array_to_ruby(elements)
    a = []
    elements.each { |e|
      index_str = e.attributes['name']
      a[index_str.to_i] = element_to_ruby(e)
    }
    a
  end

  def regex_to_ruby(elements)
    pattern = elements['pattern'].text
    options_str = elements['options'].text || ''

    options = 0
    options |= Regexp::IGNORECASE if options_str.include?('i')
    options |= Regexp::MULTILINE if options_str.include?('m')
    options |= Regexp::EXTENDED if options_str.include?('x')
    Regexp.new(pattern, options)
  end

  def dbref_to_ruby(elements)
    ns = elements['ns'].text
    oid_str = elements['oid'].text
    DBRef.new(nil, nil, nil, ns, ObjectID.from_string(oid_str))
  end

  # Round-trip comparisons of Ruby-to-BSON and back.
  # * Take the objects that were read from XML
  # * Turn them into BSON bytes
  # * Compare that with the BSON files we have
  # * Turn those BSON bytes back in to Ruby objects
  # * Turn them back into BSON bytes
  # * Compare that with the BSON files we have (or the bytes that were already
  #   generated)
  def test_round_trip
    @@ruby.each { |name, obj|
      File.open(File.join(HERE, 'data', "#{name}.bson"), 'r') { |f|
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
        assert_equal bson.length, bson_from_ruby.length
        assert_equal bson, bson_from_ruby

        # Turn those BSON bytes back into a Ruby object
        obj_from_bson = BSON.new.deserialize(ByteBuffer.new(bson_from_ruby))
        assert_kind_of OrderedHash, obj_from_bson

        # Turn that Ruby object into BSON and compare it to the original BSON
        # bytes.
        bson_from_ruby = BSON.new.serialize(obj_from_bson).to_a
        assert_equal bson.length, bson_from_ruby.length
        assert_equal bson, bson_from_ruby
      }
    }
  end

end
