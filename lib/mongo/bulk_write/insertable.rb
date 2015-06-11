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

    # Defines behavior for validating and combining insert bulk write operations.
    #
    # @since 2.0.0.
    module Insertable

      private

      def validate_insert_ops!(type, inserts)
        if inserts.empty?
          raise Error::InvalidBulkOperation.new(type, inserts)
        end
        inserts.each do |i|
          unless valid_doc?(i)
            raise Error::InvalidBulkOperation.new(type, i)
          end
        end
      end

      def insert_one(op, server)
        validate_insert_ops!(__method__, op[:insert_one])
        Operation::Write::BulkInsert.new(
          :documents => op[:insert_one].flatten,
          :db_name => database.name,
          :coll_name => @collection.name,
          :write_concern => write_concern,
          :ordered => ordered?
        ).execute(server.context)
      end
    end
  end
end