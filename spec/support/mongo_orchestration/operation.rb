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

require 'support/mongo_orchestration/operation/client_operation'
require 'support/mongo_orchestration/operation/mo_operation'

module Mongo

  module MongoOrchestration

    module Operation

      extend self

      def get(spec, config)
        if config.keys.include?('clientOperation')
          ClientOperation.new(spec.client, config['clientOperation'])
        elsif config.keys.include?('MOOperation')
          MOOperation.new(spec.mo, config['MOOperation'])
        elsif config.keys.include?('wait')
          Wait.new(config['wait'])
        end
      end
    end
  end
end
