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

    # Defines behavior for validating and combining update bulk write operations.
    #
    # @since 2.0.0.
    module Updatable

      private

      def update_doc?(doc)
        !doc.empty? &&
          doc.respond_to?(:keys) &&
          doc.keys.first.to_s =~ /^\$/
      end

      def validate_update_op!(type, u)
        unless u[:find] && u[:update] && update_doc?(u[:update])
          raise Error::InvalidBulkOperation.new(type, u)
        end
      end

      def updates(ops, type)
        multi = type == :update_many
        ops.collect do |u|
          validate_update_op!(type, u)
          { q: u[:find],
            u: u[:update],
            multi: multi,
            upsert: u.fetch(:upsert, false)
          }
        end
      end

      def update(ops, type, server)
        Operation::Write::BulkUpdate.new(
          :updates => updates(ops, type),
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(server.context)
      end

      def update_one(op, server)
        update(op[:update_one], __method__, server)
      end

      def update_many(op, server)
        update(op[:update_many], __method__, server)
      end
    end
  end
end