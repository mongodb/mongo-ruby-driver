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

class SSLTest < Test::Unit::TestCase
  include Mongo
  include SSLTests

  def setup
    @client_class     = MongoClient
    @uri_info         = 'server'
    @connect_info     = ['server', 27017]
    @bad_connect_info = ['localhost', 27017]
  end

end
