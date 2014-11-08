# Copyright (C) 2014 MongoDB, Inc.
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

module SCRAMTests

  def test_scram_authenticate
    @client.clear_auths
    assert @db.authenticate(TEST_USER, TEST_USER_PWD, nil, 'admin', 'SCRAM-SHA-1')
  end
end
