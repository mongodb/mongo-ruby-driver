# --
# Copyright (C) 2008-2009 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ++

require 'rexml/document'
require 'mongo'

# @deprecated
# Converts a .xson file (an XML file that describes a Mongo-type document) to
# an OrderedHash.
class XMLToRuby

  include Mongo

  def xml_to_ruby(io)
    warn "XMLToRuby is deprecated. The .xson format is not longer in use."
    doc = REXML::Document.new(io)
    doc_to_ruby(doc.root.elements['doc'])
  end

  protected

  def element_to_ruby(e)
    warn "XMLToRuby is deprecated. The .xson format is not longer in use."
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
    when 'string'
      e.text.to_s
    when 'code'
      Code.new(e.text.to_s)
    when 'binary'
      bin = Binary.new
      decoded = Base64.decode64(e.text.to_s)
      decoded.each_byte { |b| bin.put(b) }
      bin
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
    when 'null'
      nil
    when 'doc'
      doc_to_ruby(e)
    else
      raise "Unknown type #{type} in element with name #{e.attributes['name']}"
    end
  end

  def doc_to_ruby(element)
    warn "XMLToRuby is deprecated. The .xson format is not longer in use."
    oh = OrderedHash.new
    element.elements.each { |e| oh[e.attributes['name']] = element_to_ruby(e) }
    oh
  end

  def array_to_ruby(elements)
    warn "XMLToRuby is deprecated. The .xson format is not longer in use."
    a = []
    elements.each { |e|
      index_str = e.attributes['name']
      a[index_str.to_i] = element_to_ruby(e)
    }
    a
  end

  def regex_to_ruby(elements)
    warn "XMLToRuby is deprecated. The .xson format is not longer in use."
    pattern = elements['pattern'].text
    options_str = elements['options'].text || ''

    options = 0
    options |= Regexp::IGNORECASE if options_str.include?('i')
    options |= Regexp::MULTILINE if options_str.include?('m')
    options |= Regexp::EXTENDED if options_str.include?('x')
    Regexp.new(pattern, options)
  end

  def dbref_to_ruby(elements)
    warn "XMLToRuby is deprecated. The .xson format is not longer in use."
    ns = elements['ns'].text
    oid_str = elements['oid'].text
    DBRef.new(ns, ObjectID.from_string(oid_str))
  end

end
