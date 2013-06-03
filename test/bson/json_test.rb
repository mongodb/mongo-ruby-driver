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
require 'json'

class JSONTest < Test::Unit::TestCase

  # This test passes when run by itself but fails
  # when run as part of the whole test suite.
  def test_object_id_as_json
    #warn "Pending test object id as json"
    #id = BSON::ObjectId.new

    #obj = {'_id' => id}
    #assert_equal "{\"_id\":#{id.to_json}}", obj.to_json
  end

end
