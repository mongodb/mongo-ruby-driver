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
    module Updatable

      private

      def validate_replace_doc!(r)
        unless r[:find] && r[:replacement] && replacement_doc?(r[:replacement])
          raise Error::InvalidBulkOperation.new(__method__, r)
        end
      end

      def replacement_doc?(doc)
        doc.respond_to?(:keys) && doc.keys.all?{|key| key !~ /^\$/}
      end

      def update_doc?(doc)
        !doc.empty? &&
          doc.respond_to?(:keys) &&
          doc.keys.first.to_s =~ /^\$/
      end

      def updates(ops, multi)
        ops.collect do |u|
          unless u[:find] && u[:update] && update_doc?(u[:update])
            raise Error::InvalidBulkOperation.new(__method__, u)
          end
          { q: u[:find],
            u: u[:update],
            multi: multi,
            upsert: u[:upsert]
          }
        end
      end

      def update(ops, multi, server)
        Operation::Write::BulkUpdate.new(
          :updates => updates(ops, multi),
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(server.context)
      end

      def update_one(op, server)
        update(op[:update_one], false, server)
      end

      def update_many(op, server)
        update(op[:update_many], true, server)
      end

      def replaces(ops)
        ops.collect do |r|
          validate_replace_doc!(r)
          { q: r[:find],
            u: r[:replacement],
            multi: false,
            upsert: r.fetch(:upsert, false)
          }
        end
      end

      def replace_one(op, server)
        Operation::Write::BulkUpdate.new(
          :updates => replaces(op[:replace_one]),
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(server.context)
      end

    end
  end
end