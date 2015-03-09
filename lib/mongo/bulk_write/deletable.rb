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
    module Deletable

      private

      def validate_delete_doc!(d)
        raise Error::InvalidBulkOperation.new(__method__, d) unless valid_doc?(d)
      end

      def deletes(ops, limit)
        ops.collect do |d|
          validate_delete_doc!(d)
          { q: d, limit: limit }
        end
      end

      def delete(ops, limit, server)
        Operation::Write::BulkDelete.new(
          :deletes => deletes(ops, limit),
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(server.context)
      end

      def delete_one(op, server)
        delete(op[:delete_one], 1, server)
      end

      def delete_many(op, server)
        delete(op[:delete_many], 0, server)
      end
    end
  end
end