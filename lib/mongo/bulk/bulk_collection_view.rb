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

      # Initialize the BulkCollectionView object.
      # Encapsulates any kind of write operation that requires a selector.
      #
      # @params [ Mongo::Bulk::BulkWrite ] bulk_write The +BulkWrite+ object
      #   that will handle the actual execution of the batch operations.
      # @params [ Hash ] selector The selector associated with the write
      #   operation.
      #
      # @since 2.0.0
      def initialize(bulk_write, selector)
        @bulk_write = bulk_write
        @selector   = selector
        @upsert     = false
      end

      # Set the upsert flag for the write operation.
      #
      # @return [ self ] This object so that methods can be chained.
      #
      # @since 2.0.0
      def upsert
        @upsert = true
        self
      end

      # Update one document that matches the selector.
      #
      # @params [ Hash ] update_doc The document representing the update.
      #
      # @return [ self ] This object so that methods can be chained.
      #
      # @since 2.0.0
      def update_one(update_doc)
        raise Exception unless update_doc?(update_doc)
        multi_or_single_update(false, update_doc)
      end

      # Update all documents matching the selector.
      #
      # @params [ Hash ] update_doc The document representing the update.
      #
      # @return [ self ] This object so that methods can be chained.
      #
      # @since 2.0.0
      def update(update_doc)
        raise Exception unless update_doc?(update_doc)
        multi_or_single_update(true, update_doc)
      end

      # Replace one document matching the selector.
      #
      # @params [ Hash ] replace_doc The document representing the
      #   replacement.
      #
      # @return [ self ] This object so that methods can be chained.
      #
      # @since 2.0.0
      def replace_one(replace_doc)
        raise Exception unless replace_doc?(replace_doc)
        multi_or_single_update(false, replace_doc)
      end

      # Remove one document matching the selector.
      #
      # @return [ self ] This object so that methods can be chained.
      #
      # @since 2.0.0
      def remove_one
        multi_or_single_remove(false)
      end

      # Remove all documents matching the selector.
      #
      # @return [ self ] This object so that methods can be chained.
      #
      # @since 2.0.0
      def remove
        multi_or_single_remove(true)
      end

      private

      # Is the document a valid update document?
      # The first key must start with '$' and the document cannot be empty.
      #
      # @return [ true, false ] Whether the doc is a valid update doc.
      #
      # @since 2.0.0
      def update_doc?(doc)
        !doc.empty? && doc.keys.first.to_s =~ /^\$/
      end

      # Is the document a valid replacement document?
      # No top-level keys may start with '$'.
      #
      # @return [ true, false ] Whether the doc is a valid replacement doc.
      #
      # @since 2.0.0
      def replace_doc?(doc)
        doc.keys.all?{|key| key !~ /^\$/}
      end

      # Either do a multi update or a single update.
      #
      # @params [ true, false ] multi Whether the operation is a multi or single
      #   update.
      # @params [ Hash ] update_doc The document representing the update. Either
      #   a replacement or update document.
      #
      # @return [ Mongo::Bulk::BulkWrite ] The bulk write object.
      #
      # @since 2.0.0
      def multi_or_single_update(multi = false, update_doc)
        raise Exception unless @selector

        spec = { :updates      => [ {:q => @selector,
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

      # Either do a multi remove or a single remove.
      #
      # @params [ true, false ] multi Whether the remove is a multi or single
      #   remove.
      #
      # @return [ Mongo::Bulk::BulkWrite ] The bulk write object.
      #
      # @since 2.0.0
      def multi_or_single_remove(multi = false)
        raise Exception unless @selector
        
        spec = { :deletes       => [ {:q => @selector,
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