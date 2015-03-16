# Copyright (C) 2014-2015 MongoDB, Inc.
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

module Mongo
  module MongoOrchestration
    module Operation

      class ClientOperation

        OPERATIONS = {
                       'insertOne' => :insert_one,
                       'find' => :find
                     }.freeze

        def initialize(client, config)
          @client = client
          @outcome = config['outcome']
          @operation = config['operation']
          @doc = config['doc']
        end

        def run
          process do
            send(OPERATIONS[@operation])
          end
        end

        private

        def insert_one
          @client['test'].insert_one(@doc)
        end

        def find
          @client['test'].find.to_a
        end

        def process
          begin
            result = yield
          rescue Mongo::Error
            if @outcome['ok'] == 1
              return false
            end
          end
          return result unless result.respond_to?(:successful?)
          result.successful?
        end
      end
    end
  end
end