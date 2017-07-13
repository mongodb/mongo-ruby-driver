# Copyright (C) 2009-2014 MongoDB Inc.
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

require "spec_helper"

describe Mongo::Protocol::Registry do

  describe ".get" do

    context "when the type has a correspoding class" do

      before do
        described_class.register(Mongo::Protocol::Query::OP_CODE, Mongo::Protocol::Query)
      end

      let(:klass) do
        described_class.get(Mongo::Protocol::Query::OP_CODE, "message")
      end

      it "returns the class" do
        expect(klass).to eq(Mongo::Protocol::Query)
      end
    end

    context "when the type has no corresponding class" do

      it "raises an error" do
        expect {
          described_class.get(-100, "message")
        }.to raise_error(Mongo::Protocol::Registry::UnsupportedType)
      end
    end
  end
end
