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

$LOAD_PATH[0,0] = File.join(File.dirname(__FILE__), '..', 'lib')
require 'mongo'
require 'test/unit'

class TestQueryMessage < Test::Unit::TestCase
  
  include Mongo

  def test_timeout_opcodes
    @timeout       = true
    @query         = Query.new({}, nil, 0, 0, nil, nil, nil, @timeout)
    @query_message = QueryMessage.new('db', 'collection', @query)
    buf = @query_message.buf.instance_variable_get(:@buf)
    assert_equal 0, buf[16]


    @timeout       = false
    @query         = Query.new({}, nil, 0, 0, nil, nil, nil, @timeout)
    @query_message = QueryMessage.new('db', 'collection', @query)
    buf = @query_message.buf.instance_variable_get(:@buf)
    assert_equal 16, buf[16]
  end

end
