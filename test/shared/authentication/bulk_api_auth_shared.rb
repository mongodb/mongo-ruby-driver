# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0x
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

module BulkAPIAuthTests

  include Mongo

  def init_auth_bulk
    # Set up the test db
    @collection = @db["bulk-api-auth-tests"]

    # db user can insert but not remove
    res = BSON::OrderedHash.new
    res[:db] = @db.name
    res[:collection] = ""

    cmd = BSON::OrderedHash.new
    cmd[:createRole] = "insertOnly"
    cmd[:privileges] = [{:resource => res, :actions => [ "insert", "find" ]}]
    cmd[:roles] = []
    @db.command(cmd)
    @db.add_user('insertOnly', 'password', nil, :roles => ['insertOnly'])

    # db user can insert and remove
    cmd = BSON::OrderedHash.new
    cmd[:createRole] = "insertAndRemove"
    cmd[:privileges] = [{:resource => res, :actions => [ "insert", "remove", "find" ]}]
    cmd[:roles] = []
    @db.command(cmd)
    @db.add_user('insertAndRemove', 'password', nil, :roles => ['insertAndRemove'])

    @admin.logout
  end

  def clear_collection(collection)
    @admin.authenticate(TEST_USER, TEST_USER_PWD)
    collection.remove
    @admin.logout
  end

  def teardown_bulk
    remove_all_users_and_roles(@db)
    remove_all_users_and_roles(@admin)
  end

  def remove_all_users_and_roles(database)
    @admin.authenticate(TEST_USER, TEST_USER_PWD)
    if @version < '2.5.3'
      database['system.users'].remove
    else
      database.command({:dropAllRolesFromDatabase => 1})
      # Don't delete the TEST_USER from the TEST_DB, it is needed for future tests
      database.command({:dropAllUsersFromDatabase => 1}) unless database.name == TEST_DB
    end
    @admin.logout
  end

  def test_auth_no_error
    return unless @version >= '2.5.3'
    init_auth_bulk
    with_write_commands_and_operations(@db.connection) do |wire_version|
      clear_collection(@collection)
      @db.authenticate('insertAndRemove', 'password')
      bulk = @collection.initialize_ordered_bulk_op
      bulk.insert({:a => 1})
      bulk.find({:a => 1}).remove_one

      result = bulk.execute
      assert_match_document(
          {
              "ok" => 1,
              "nInserted" => 1,
              "nRemoved" => 1
          }, result, "wire_version:#{wire_version}")
      assert_equal 0, @collection.count
      @db.logout
    end
    teardown_bulk
  end

  def test_auth_error
    return unless @version >= '2.5.3'
    init_auth_bulk
    with_write_commands_and_operations(@db.connection) do |wire_version|
      clear_collection(@collection)
      @db.authenticate('insertOnly', 'password')
      bulk = @collection.initialize_ordered_bulk_op
      bulk.insert({:a => 1})
      bulk.find({:a => 1}).remove
      bulk.insert({:a => 2})

      ex = assert_raise Mongo::BulkWriteError do
        bulk.execute
      end
      result = ex.result
      assert_match_document(
          {
              "ok" => 1,
              "n" => 1,
              "writeErrors" =>
                  [{
                       "index" => 1,
                       "code" => 13,
                       "errmsg" => /not authorized/
                  }],
              "code" => 65,
              "errmsg" => "batch item errors occurred",
              "nInserted" => 1
           }, result, "wire_version:#{wire_version}")
      assert_equal 1, @collection.count
      @db.logout
    end
    teardown_bulk
  end

  def test_auth_error_unordered
    return unless @version >= '2.5.3'
    init_auth_bulk
    with_write_commands_and_operations(@db.connection) do |wire_version|
      clear_collection(@collection)
      @db.authenticate('insertOnly', 'password')
      bulk = @collection.initialize_unordered_bulk_op
      bulk.insert({:a => 1})
      bulk.find({:a => 1}).remove_one
      bulk.insert({:a => 2})

      ex = assert_raise Mongo::BulkWriteError do
        bulk.execute
      end
      result = ex.result
      assert_equal 1, result["writeErrors"].length
      assert_equal 2, result["n"]
      assert_equal 2, result["nInserted"]
      assert_equal 2, @collection.count
      @db.logout
    end
    teardown_bulk
  end

  def test_duplicate_key_with_auth_error
    return unless @version >= '2.5.3'
    init_auth_bulk
    with_write_commands_and_operations(@db.connection) do |wire_version|
      clear_collection(@collection)
      @db.authenticate('insertOnly', 'password')
      bulk = @collection.initialize_ordered_bulk_op
      bulk.insert({:_id => 1, :a => 1})
      bulk.insert({:_id => 1, :a => 2})
      bulk.find({:a => 1}).remove_one

      ex = assert_raise Mongo::BulkWriteError do
        bulk.execute
      end
      result = ex.result
      assert_match_document(
          {
              "ok" => 1,
              "n" => 1,
              "writeErrors" =>
                  [{
                       "index" => 1,
                       "code" => 11000,
                       "errmsg" => /duplicate key error/
                  }],
              "code" => 65,
              "errmsg" => "batch item errors occurred",
              "nInserted" => 1
           }, result, "wire_version:#{wire_version}")
      assert_equal 1, @collection.count
      @db.logout
    end
    teardown_bulk
  end

  def test_duplicate_key_with_auth_error_unordered
    return unless @version >= '2.5.3'
    init_auth_bulk
    with_write_commands_and_operations(@db.connection) do |wire_version|
      clear_collection(@collection)
      @db.authenticate('insertOnly', 'password')
      bulk = @collection.initialize_unordered_bulk_op
      bulk.insert({:_id => 1, :a => 1})
      bulk.insert({:_id => 1, :a => 1})
      bulk.find({:a => 1}).remove_one

      ex = assert_raise Mongo::BulkWriteError do
        bulk.execute
      end
      result = ex.result
      assert_equal 2, result["writeErrors"].length
      assert_equal 1, result["n"]
      assert_equal 1, result["nInserted"]
      assert_equal 1, @collection.count
      @db.logout
    end
    teardown_bulk
  end

  def test_write_concern_error_with_auth_error
    with_no_replication(@db.connection) do
      return unless @version >= '2.5.3'
      init_auth_bulk
      with_write_commands_and_operations(@db.connection) do |wire_version|
        clear_collection(@collection)
        @db.authenticate('insertOnly', 'password')
        bulk = @collection.initialize_ordered_bulk_op
        bulk.insert({:_id => 1, :a => 1})
        bulk.insert({:_id => 2, :a => 1})
        bulk.find({:a => 1}).remove_one
        
        ex = assert_raise Mongo::BulkWriteError do
          bulk.execute({:w => 2})
        end
        result = ex.result
        
        assert_match_document(
            {
                "ok" => 0,
                "n" => 0,
                "nInserted" => 0,
                "writeErrors" =>
                    [{
                         "index" => 0,
                         "code" => 2,
                         "errmsg" => /'w' > 1/
                    }],
                "code" => 65,
                "errmsg" => "batch item errors occurred"
             }, result, "wire_version#{wire_version}")
# Re-visit this when RUBY-731 is resolved:
        assert (@collection.count == batch_commands?(wire_version) ? 0 : 1)
        @db.logout
      end
      teardown_bulk
    end
  end

end
