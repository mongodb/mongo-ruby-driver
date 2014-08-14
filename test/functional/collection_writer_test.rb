# Copyright (C) 2009-2013 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License")
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

module Mongo
  class Collection
    public :batch_write
  end
  class CollectionWriter
    public :sort_by_first_sym, :ordered_group_by_first
  end
end

class CollectionWriterTest < Test::Unit::TestCase

  DATABASE_NAME = 'ruby_test_collection_writer'
  COLLECTION_NAME = 'test'

  def default_setup
    @client = MongoClient.from_uri(TEST_URI)
    @db = @client[DATABASE_NAME]
    @collection = @db[COLLECTION_NAME]
    @collection.drop
  end

  context "Bulk API Execute" do
    setup do
      default_setup
    end

    should "sort_by_first_sym for grouping unordered ops" do
      pairs = [
          [:insert, {:n => 0}],
          [:update, {:n => 1}], [:update, {:n => 2}],
          [:delete, {:n => 3}],
          [:insert, {:n => 5}], [:insert, {:n => 6}], [:insert, {:n => 7}],
          [:update, {:n => 8}],
          [:delete, {:n => 9}], [:delete, {:n => 10}]
      ]
      result = @collection.command_writer.sort_by_first_sym(pairs)
      expected = [
          :delete, :delete, :delete,
          :insert, :insert, :insert, :insert,
          :update, :update, :update
      ]
      assert_equal expected, result.collect{|first, rest| first}
    end

    should "calculate ordered_group_by_first" do
      pairs = [
          [:insert, {:n => 0}],
          [:update, {:n => 1}], [:update, {:n => 2}],
          [:delete, {:n => 3}],
          [:insert, {:n => 5}], [:insert, {:n => 6}], [:insert, {:n => 7}],
          [:update, {:n => 8}],
          [:delete, {:n => 9}], [:delete, {:n => 10}]
      ]
      result = @collection.command_writer.ordered_group_by_first(pairs)
      expected = [
          [:insert, [{:n => 0}]],
          [:update, [{:n => 1}, {:n => 2}]],
          [:delete, [{:n => 3}]],
          [:insert, [{:n => 5}, {:n => 6}, {:n => 7}]],
          [:update, [{:n => 8}]],
          [:delete, [{:n => 9}, {:n => 10}]]
      ]
      assert_equal expected, result
    end

  end
end
