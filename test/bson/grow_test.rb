# encoding: utf-8

# Copyright (C) 2013 MongoDB, Inc.
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

module BSON
  class ByteBuffer
    def to_ruby
      BSON::BSON_CODER.deserialize(self)
    end
  end
end

class Hash
  def to_bson
    BSON::BSON_CODER.serialize(self, false, true)
  end
end

class TestCollection < Test::Unit::TestCase

  def setup
    @doc_a = {"a"=>0}
    @bson_a = @doc_a.to_bson
    @doc_b = {"b"=>1}
    @bson_b = @doc_b.to_bson
    @doc_a_b = @doc_a.merge(@doc_b)
  end

  def test_to_e
    assert_equal "\x10a\x00\x00\x00\x00\x00", {:a=>0}.to_bson.to_e
  end

  def test_finish_one_bang
    message = BSON::ByteBuffer.new
    message.put_int(0)
    message.put_binary(@bson_a.to_e)
    message.finish_one!
    assert_equal @doc_a, message.to_ruby
  end

  def test_unfinish_bang
    message = @bson_a.unfinish!
    message.put_binary(@bson_b.to_e)
    message.finish_one!
    assert_equal @doc_a_b, message.to_ruby
  end

  def test_grow_bang
    message = @bson_a.unfinish!.grow!(@bson_b).finish_one!
    assert_equal @doc_a_b, message.to_ruby
  end

  def test_grow
    message = @bson_a.grow(@bson_b)
    assert_equal @doc_a_b, message.to_ruby
  end

  def test_do_bang_and_finish_bang
    message = @bson_a.unfinish!.b_do!("c").finish!
    assert_equal({"a"=>0, "c"=>{}}, message.to_ruby)
  end

  def test_b_do_bang_with_grow_bang
    message = @bson_a.unfinish!.b_do!("c").grow!({"0"=>99}.to_bson).finish!
    assert_equal({"a"=>0, "c"=>{"0"=>99}}, message.to_ruby)
  end

  def test_do_with_grow
    message = @bson_a.b_do("c").grow({"0"=>99}.to_bson)
    assert_equal({"a"=>0, "c"=>{"0"=>99}}, message.to_ruby)
  end

  def test_doc_bang_grow_bang
    message = @bson_a.unfinish!.doc!("c").grow!({"0"=>99}.to_bson).finish!
    assert_equal({"a"=>0, "c"=>{"0"=>99}}, message.to_ruby)
  end

  def test_doc_grow
    message = @bson_a.doc("c").grow({"0"=>99}.to_bson)
    assert_equal({"a"=>0, "c"=>{"0"=>99}}, message.to_ruby)
  end

  def test_array_grow_bang
    message = @bson_a.unfinish!.array!("c").grow!(99).grow!(101).finish!
    assert_equal({"a"=>0, "c"=>[99, 101]}, message.to_ruby)
  end

  def test_array_grow
    message = @bson_a.array("c").grow(99).grow(101)
    assert_equal({"a"=>0, "c"=>[99, 101]}, message.to_ruby)
  end

  def test_b_end_bang
    message = @bson_a.unfinish!.array!("c").grow!(99).b_end!.grow!(@bson_b).finish!
    assert_equal({"a"=>0, "b"=>1, "c"=>[99]}, message.to_ruby)
  end

  def test_b_end
    message = @bson_a.array("c").grow(99).b_end.grow(@bson_b)
    assert_equal({"a"=>0, "c"=>[99], "b"=>1}, message.to_ruby)
  end

end
