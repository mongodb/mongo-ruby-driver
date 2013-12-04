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
require 'shared/ssl_shared'

class ReplicaSetSSLTest < Test::Unit::TestCase
  include Mongo
  include SSLTests

  SEEDS     = ['server:3000','server:3001','server:3002']
  BAD_SEEDS = ['localhost:3000','localhost:3001','localhost:3002']

  def setup
    @client_class     = MongoReplicaSetClient
    @uri_info         = SEEDS.join(',')
    @connect_info     = SEEDS
    @bad_connect_info = BAD_SEEDS
  end

end
