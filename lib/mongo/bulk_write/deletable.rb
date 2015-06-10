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

    # Defines behavior for validating and combining delete bulk write operations.
    #
    # @since 2.0.0.
    module Deletable

      private

      def validate_delete_op!(type, d)
        raise Error::InvalidBulkOperation.new(type, d) unless valid_doc?(d)
      end

      def deletes(ops, type)
        limit = (type == :delete_one) ? 1 : 0
        ops.collect do |d|
          validate_delete_op!(type, d)
          { q: d, limit: limit }
        end
      end

      def delete(ops, type, server)
        Operation::Write::BulkDelete.new(
          :deletes => deletes(ops, type),
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(server.context)
      end

      def delete_one(op, server)
        delete(op[:delete_one], __method__, server)
      end

      def delete_many(op, server)
        delete(op[:delete_many], __method__, server)
      end
    end
  end
end