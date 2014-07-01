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

module Mongo
  module Bulk
    class BulkCollectionView

      def initialize(bulk_write, query)
        @bulk_write = bulk_write
        @query      = query
        @upsert     = false
      end

      # set flags
      def upsert
        @upsert = true
        self
      end

      # terminators
      def update_one(update_doc)
        raise Exception unless update_doc?(update_doc)
        multi_or_single_update(false, update_doc)
      end

      def update(update_doc)
        raise Exception unless update_doc?(update_doc)
        multi_or_single_update(true, update_doc)
      end

      def replace_one(replace_doc)
        raise Exception unless replace_doc?(replace_doc)
        multi_or_single_update(false, replace_doc)
      end

      def remove_one
        multi_or_single_remove(false)
      end

      def remove
        multi_or_single_remove(true)
      end

      private

      def update_doc?(doc)
        !doc.empty? && doc.keys.first.to_s =~ /^\$/
      end

      def replace_doc?(doc)
        doc.keys.all?{|key| key !~ /^\$/}
      end

      def multi_or_single_update(multi = false, update_doc)
        raise Exception unless @query

        spec = { :updates      => [ {:q => @query,
                                     :u => update_doc,
                                     :multi => multi,
                                     :upsert => upsert }],
                 :db_name       => @bulk_write.db_name,
                 :coll_name     => @bulk_write.coll_name,
                 :ordered       => @bulk_write.ordered? }

        op = Mongo::Operation::Write::Update.new(spec)
        @bulk_write.tap do |b|
          b << op
        end
      end

      def multi_or_single_remove(multi = false)
        raise Exception unless @query
        
        spec = { :deletes       => [ {:q => @query,
                                      :limit => multi ? nil : 1}],
                 :db_name       => @bulk_write.db_name,
                 :coll_name     => @bulk_write.coll_name,
                 :ordered       => @bulk_write.ordered? }

        op = Mongo::Operation::Write::Delete.new(spec)
        @bulk_write.tap do |b|
          b << op
        end
      end
    end
  end
end