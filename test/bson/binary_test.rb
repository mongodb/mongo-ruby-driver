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

class BinaryTest < Test::Unit::TestCase
  def setup
    @data = ("THIS IS BINARY " * 50).unpack("c*")
  end

  def test_do_not_display_binary_data
    binary = BSON::Binary.new(@data)
    assert_equal "<BSON::Binary:#{binary.object_id}>", binary.inspect
  end
end
