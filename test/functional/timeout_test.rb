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

class TimeoutTest < Test::Unit::TestCase

  def test_op_timeout
    connection = standard_connection(:op_timeout => 0.5)

    admin = connection.db('admin')

    command = {:eval => "sleep(100)"}
    # Should not timeout
    assert admin.command(command)

    # Should timeout
    command = {:eval => "sleep(1000)"}
    assert_raise Mongo::OperationTimeout do
      admin.command(command)
    end
  end

  def test_external_timeout_does_not_leave_socket_in_bad_state
    client = standard_connection
    db     = client[TEST_DB]
    coll   = db['timeout-tests']

    # prepare the database
    coll.drop
    coll.insert({:a => 1})

    # use external timeout to mangle socket
    begin
      Timeout::timeout(0.5) do
        db.command({:eval => "sleep(1000)"})
      end
    rescue Timeout::Error
      #puts "Thread timed out and has now mangled the socket"
    end

    assert_nothing_raised do
      coll.find_one
    end
  end

end
