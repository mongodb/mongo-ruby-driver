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

module Mongo
  module Operation
    # representation of the findAndModify operation.
    # @api semipublic
    class FindAndModify

      def initialize(scope, opts = {})
        @scope = scope
        @opts = opts
        valid?
      end

      def execute
        result = cluster.execute(op)
        value(result)
      end

      private

      def valid?
        raise Exception, 'cannot specify skip' if @scope.skip
        if update && !update_doc?(update)
          raise Exception, 'missing update operator'
        elsif replacement && update_doc?(replacement)
          raise Exception, 'replacement document cannot have' +
            ' $ in the first key'
        end
        true
      end

      def value(result)
        result['value'] == 'null' ? nil : result['value']
      end

      # This will change when we model Operation
      def op
        { :findandmodify => collection.name,
          :query => query,
          :new => new?,
          :fields => fields,
          :upsert => upsert?,
          :sort => sort,
          :update => update || replacement || {},
          :remove => remove?
        }
      end

      def collection
        @scope.collection
      end

      def query
        @scope.selector
      end

      def new?
        !!@opts[:new]
      end

      def fields
        @opts[:fields] || {}
      end

      def upsert?
        !!@opts[:upsert]
      end

      def sort
        @scope.sort || {}
      end

      def update
        @opts[:update]
      end

      def replacement
        @opts[:replace]
      end

      def remove?
        !!@opts[:remove]
      end

      def update_doc?(doc)
        !doc.empty? && doc.keys.first.to_s =~ /^\$/
      end

      def cluster
        @scope.cluster
      end
    end
  end
end
