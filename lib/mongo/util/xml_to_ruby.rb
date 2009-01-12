# --
# Copyright (C) 2008-2009 10gen Inc.
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Affero General Public License, version 3, as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
# for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# ++

require 'rexml/document'
require 'mongo'

# Converts a .xson file (an XML file that describes a Mongo-type document) to
# an OrderedHash.
class XMLToRuby

  include XGen::Mongo::Driver

  def xml_to_ruby(io)
    doc = REXML::Document.new(io)
    doc_to_ruby(doc.root.elements['doc'])
  end

  protected

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
    when 'binary'
      Base64.decode64(e.text.to_s).to_mongo_binary
    when 'symbol'
      e.text.to_s.intern
    when 'boolean'
      e.text.to_s == 'true'
    when 'array'
      array_to_ruby(e.elements)
    when 'date'
      Time.at(e.text.to_f / 1000.0)
    when 'regex'
      regex_to_ruby(e.elements)
    when 'null', 'undefined'
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

end
