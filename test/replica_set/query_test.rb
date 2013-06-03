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

class ReplicaSetQueryTest < Test::Unit::TestCase

  def setup
    ensure_cluster(:rs)
    @client = MongoReplicaSetClient.new @rs.repl_set_seeds
    @db = @client.db(MONGO_TEST_DB)
    @db.drop_collection("test-sets")
    @coll = @db.collection("test-sets")
  end

  def teardown
    @client.close if @conn
  end

  def test_query
    @coll.save({:a => 20}, :w => 3)
    @coll.save({:a => 30}, :w => 3)
    @coll.save({:a => 40}, :w => 3)
    results = []
    @coll.find.each {|r| results << r}
    [20, 30, 40].each do |a|
      assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a}"
    end

    @rs.primary.stop

    results = []
    rescue_connection_failure do
      @coll.find.each {|r| results << r}
      [20, 30, 40].each do |a|
        assert results.any? {|r| r['a'] == a}, "Could not find record for a => #{a}"
      end
    end
  end

  # Create a large collection and do a secondary query that returns
  # enough records to require sending a GETMORE. In between opening
  # the cursor and sending the GETMORE, do a :primary query. Confirm
  # that the cursor reading from the secondary continues to talk to
  # the secondary, rather than trying to read the cursor from the
  # primary, where it does not exist.
  # def test_secondary_getmore
  #   200.times do |i|
  #     @coll.save({:a => i}, :w => 3)
  #   end
  #   as = []
  #   # Set an explicit batch size, in case the default ever changes.
  #   @coll.find({}, { :batch_size => 100, :read => :secondary }) do |c|
  #     c.each do |result|
  #       as << result['a']
  #       @coll.find({:a => result['a']}, :read => :primary).map
  #     end
  #   end
  #   assert_equal(as.sort, 0.upto(199).to_a)
  # end

end
