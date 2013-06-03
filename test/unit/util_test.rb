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

require File.expand_path("../../test_helper", __FILE__)

class UtilTest < Test::Unit::TestCase
  context "Support" do
    context ".secondary_ok?" do
      should "return false for mapreduces with a string for out" do
        assert_equal false, Mongo::Support.secondary_ok?(BSON::OrderedHash[
            'mapreduce', 'test-collection',
            'out', 'new-test-collection'
          ])
      end

      should "return false for mapreduces replacing a collection" do
        assert_equal false, Mongo::Support.secondary_ok?(BSON::OrderedHash[
            'mapreduce', 'test-collection',
            'out', BSON::OrderedHash['replace', 'new-test-collection']
          ])
      end

      should "return false for mapreduces replacing the inline collection" do
        assert_equal false, Mongo::Support.secondary_ok?(BSON::OrderedHash[
            'mapreduce', 'test-collection',
            'out', 'inline'
          ])
      end

      should "return true for inline output mapreduces when inline is a symbol" do
        assert_equal true, Mongo::Support.secondary_ok?(BSON::OrderedHash[
            'mapreduce', 'test-collection',
            'out', BSON::OrderedHash[:inline, 'true']
          ])
      end

      should "return true for inline output mapreduces when inline is a string" do
        assert_equal true, Mongo::Support.secondary_ok?(BSON::OrderedHash[
            'mapreduce', 'test-collection',
            'out', BSON::OrderedHash['inline', 'true']
          ])
      end

      should 'return true for count' do
        assert_equal true, Mongo::Support.secondary_ok?(BSON::OrderedHash[
            'count', 'test-collection',
            'query', BSON::OrderedHash['a', 'b']
          ])
      end

      should 'return false for serverStatus' do
        assert_equal false, Mongo::Support.secondary_ok?(BSON::OrderedHash[
            'serverStatus', 1
          ])
      end
    end
  end
end
