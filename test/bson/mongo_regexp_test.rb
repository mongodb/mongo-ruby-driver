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

class MongoRegexpTest < Test::Unit::TestCase

  def test_convert_regexp_to_mongo_regexp
    BSON::MongoRegexp.expects(:warn)
    regexp = Regexp.new(/.*/imx)
    mongo_regexp = BSON::MongoRegexp.from_native(regexp)
    assert_equal regexp.source, mongo_regexp.source
  end

  def test_compile_mongo_regexp
    BSON::MongoRegexp.any_instance.expects(:warn)
    mongo_regexp = BSON::MongoRegexp.new(".*", 'imx')
    regexp = mongo_regexp.unsafe_compile
    assert_equal 3, regexp.options
  end
end
