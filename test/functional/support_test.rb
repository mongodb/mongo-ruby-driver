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

class SupportTest < Test::Unit::TestCase

  def test_command_response_succeeds
    assert Support.ok?('ok' => 1)
    assert Support.ok?('ok' => 1.0)
    assert Support.ok?('ok' => true)
  end

  def test_command_response_fails
    assert !Support.ok?('ok' => 0)
    assert !Support.ok?('ok' => 0.0)
    assert !Support.ok?('ok' => 0.0)
    assert !Support.ok?('ok' => 'str')
    assert !Support.ok?('ok' => false)
  end

  def test_array_of_pairs
    hps = [["localhost", 27017], ["localhost", 27018], ["localhost", 27019]]
    assert_equal [["localhost", 27017], ["localhost", 27018], ["localhost", 27019]], Support.normalize_seeds(hps)
  end

  def test_array_of_strings
    hps = ["localhost:27017", "localhost:27018", "localhost:27019"]
    assert_equal [["localhost", 27017], ["localhost", 27018], ["localhost", 27019]], Support.normalize_seeds(hps)
  end

  def test_single_string_with_host_port
    hps = "localhost:27017"
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end

  def test_single_string_missing_port
    hps = "localhost"
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end

  def test_single_element_array_missing_port
    hps = ["localhost"]
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end

  def test_pair_doesnt_get_converted
    hps = ["localhost", 27017]
    assert_equal ["localhost", 27017], Support.normalize_seeds(hps)
  end
end
