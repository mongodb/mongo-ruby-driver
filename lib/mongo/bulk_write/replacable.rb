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
  module BulkWrite

    # Defines behavior for validating and combining replace bulk write operations.
    #
    # @since 2.0.0.
    module Replacable

      private

      def replacement_doc?(doc)
        doc.respond_to?(:keys) && doc.keys.all?{|key| key !~ /^\$/}
      end

      def validate_replace_op!(r, type)
        unless r[:find] && r[:replacement] && replacement_doc?(r[:replacement])
          raise Error::InvalidBulkOperation.new(type, r)
        end
      end

      def replace_ops(ops, type)
        ops.collect do |r|
          validate_replace_op!(r, type)
          { q: r[:find],
            u: r[:replacement],
            multi: false,
            upsert: r.fetch(:upsert, false)
          }
        end
      end

      def replace_one(op, server)
        Operation::Write::BulkUpdate.new(
          :updates => replace_ops(op[:replace_one], __method__),
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(server.context)
      end
    end
  end
end