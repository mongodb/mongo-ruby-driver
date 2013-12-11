# Copyright (C) 2009-2013 MongoDB, Inc.
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

class BSONRegexTest < Test::Unit::TestCase

  def test_convert_regexp_to_bson_regex
    regexp = Regexp.new(/.*/imx)
    bson_regex = BSON::Regex.from_native(regexp)
    assert_equal regexp.source, bson_regex.source
  end

  def test_compile_bson_regex
    bson_regex = BSON::Regex.new(".*", 'imx')
    regexp = bson_regex.try_compile
    assert_equal 3, regexp.options
  end
end
