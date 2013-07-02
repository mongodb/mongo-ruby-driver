# Copyright (C) 2013 10gen Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'test_helper'

class OrderedHashTest < Test::Unit::TestCase

  def setup
    @oh = BSON::OrderedHash.new
    @oh['c'] = 1
    @oh['a'] = 2
    @oh['z'] = 3
    @ordered_keys = %w(c a z)
  end

  def test_initialize
    a = BSON::OrderedHash.new
    a['x'] = 1
    a['y'] = 2

    b = BSON::OrderedHash['x', 1, 'y', 2]
    assert_equal a, b
  end

  def test_hash_code
    o = BSON::OrderedHash.new
    o['number'] = 50
    assert o.hash
  end

  def test_empty
    assert_equal [], BSON::OrderedHash.new.keys
  end

  def test_uniq
    list = []
    doc  = BSON::OrderedHash.new
    doc['_id']  = 'ab12'
    doc['name'] = 'test'

    same_doc = BSON::OrderedHash.new
    same_doc['_id']  = 'ab12'
    same_doc['name'] = 'test'

    list << doc
    list << same_doc

    assert_equal 2, list.size
    assert_equal 1, list.uniq.size
  end

  if !(RUBY_VERSION =~ /1.8.6/)
    def test_compatibility_with_hash
      list = []
      doc  = BSON::OrderedHash.new
      doc['_id']  = 'ab12'
      doc['name'] = 'test'

      doc2 = {}
      doc2['_id']  = 'ab12'
      doc2['name'] = 'test'
      list << doc
      list << doc2

      assert_equal 1, list.uniq.size
    end
  end

  def test_equality
    a = BSON::OrderedHash.new
    a['x'] = 1
    a['y'] = 2

    b = BSON::OrderedHash.new
    b['y'] = 2
    b['x'] = 1

    c = BSON::OrderedHash.new
    c['x'] = 1
    c['y'] = 2

    d = BSON::OrderedHash.new
    d['x'] = 2
    d['y'] = 3

    e = BSON::OrderedHash.new
    e['z'] = 1
    e['y'] = 2

    assert_equal a, c
    assert_not_equal a, b
    assert_not_equal a, d
    assert_not_equal a, e
  end

  def test_order_preserved
    assert_equal @ordered_keys, @oh.keys
  end

  def test_replace
    h1 = BSON::OrderedHash.new
    h1[:a] = 1
    h1[:b] = 2

    h2 = BSON::OrderedHash.new
    h2[:c] = 3
    h2[:d] = 4
    h1.replace(h2)

    assert_equal [:c, :d], h1.keys
    assert_equal [3, 4], h1.values
    assert h1.keys.object_id != h2.keys.object_id
  end

  def test_to_a_order_preserved
    assert_equal @ordered_keys, @oh.to_a.map {|m| m.first}
  end

  def test_order_preserved_after_replace
    @oh['a'] = 42
    assert_equal @ordered_keys, @oh.keys
    @oh['c'] = 'foobar'
    assert_equal @ordered_keys, @oh.keys
    @oh['z'] = /huh?/
    assert_equal @ordered_keys, @oh.keys
  end

  def test_each
    keys = []
    @oh.each { |k, v| keys << k }
    assert_equal keys, @oh.keys

    @oh['z'] = 42
    assert_equal keys, @oh.keys

    assert_equal @oh, @oh.each {|k,v|}
  end

  def test_values
    assert_equal [1, 2, 3], @oh.values
  end

  def test_merge
    other = BSON::OrderedHash.new
    other['f'] = 'foo'
    noob = @oh.merge(other)
    assert_equal @ordered_keys + ['f'], noob.keys
    assert_equal [1, 2, 3, 'foo'], noob.values
  end

  def test_merge_bang
    other = BSON::OrderedHash.new
    other['f'] = 'foo'
    @oh.merge!(other)
    assert_equal @ordered_keys + ['f'], @oh.keys
    assert_equal [1, 2, 3, 'foo'], @oh.values
  end

  def test_merge_bang_with_overlap
    other = BSON::OrderedHash.new
    other['a'] = 'apple'
    other['c'] = 'crab'
    other['f'] = 'foo'
    @oh.merge!(other)
    assert_equal @ordered_keys + ['f'], @oh.keys
    assert_equal ['crab', 'apple', 3, 'foo'], @oh.values
  end

  def test_merge_bang_with_hash_with_overlap
    other = Hash.new
    other['a'] = 'apple'
    other['c'] = 'crab'
    other['f'] = 'foo'
    @oh.merge!(other)
    assert_equal @ordered_keys + ['f'], @oh.keys
    assert_equal ['crab', 'apple', 3, 'foo'], @oh.values
  end

  def test_equality_with_hash
    o = BSON::OrderedHash.new
    o[:a] = 1
    o[:b] = 2
    o[:c] = 3
    r = {:a => 1, :b => 2, :c => 3}
    assert r == o
    assert o == r
  end

  def test_update
    other = BSON::OrderedHash.new
    other['f'] = 'foo'
    noob = @oh.update(other)
    assert_equal @ordered_keys + ['f'], noob.keys
    assert_equal [1, 2, 3, 'foo'], noob.values
  end

  if RUBY_VERSION < "1.9.2"
    def test_inspect_retains_order
      assert_equal "#<BSON::OrderedHash:0x#{@oh.object_id.to_s(16)} {\"c\"=>1, \"a\"=>2, \"z\"=>3}>", @oh.inspect
    end
  end

  def test_clear
    @oh.clear
    assert @oh.keys.empty?
  end

  def test_delete
    assert @oh.keys.include?('z')
    @oh.delete('z')
    assert !@oh.keys.include?('z')
  end

  def test_delete_if
    assert @oh.keys.include?('z')
    @oh.delete_if { |k,v| k == 'z' }
    assert !@oh.keys.include?('z')
    @oh.delete_if { |k, v| v > 0 }
    assert @oh.keys.empty?
  end

  def test_reject
    new = @oh.reject { |k, v| k == 'foo' }
    assert new.keys == @oh.keys

    new = @oh.reject { |k, v| k == 'z' }
    assert !new.keys.include?('z')
  end

  def test_reject_bang
    @oh.reject! { |k, v| k == 'z' }
    assert !@oh.keys.include?('z')
    assert_nil @oh.reject! { |k, v| k == 'z' }
  end

  def test_clone
    copy = @oh.clone
    assert copy.keys == @oh.keys

    copy[:foo] = 1
    assert copy.keys != @oh.keys
  end

  def test_dup
    oh2 = @oh.dup
    oh2['f'] = 9
    assert_nil @oh['f']
    assert_equal ['c', 'a', 'z'], @oh.keys
  end

  def test_extractable_options_for_ordered_hash
    assert @oh.extractable_options?
  end

  # Extractable_options should not be enabled by default for
  # classes inherited from BSON::OrderedHash
  #
  def test_extractable_options_for_ordered_hash_inherited_classes_is_false
    oh_child_class = Class.new(BSON::OrderedHash)
    assert_false oh_child_class.new.extractable_options?
  end
end
