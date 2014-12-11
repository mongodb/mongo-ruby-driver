# Copyright (C) 2009-2014 MongoDB, Inc.
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

module MongoOrchestration
  class Standalone
    include Requestable

    attr_reader :id
    attr_reader :config

    private

    def create(options = {})
      request_content = setup_config[:request_content]
      id = request_content[:id]
      if exists?(id)
        @id = id
      else
        post(setup_config[:orchestration], { body: request_content })
        @config = @response
      end
    end

    def setup_config
     {
        orchestration: "servers",
        request_content: {
                          id: "standalone",
                          name: "mongod",
                          procParams: { journal: true }
                         }
      }
    end
  end
end