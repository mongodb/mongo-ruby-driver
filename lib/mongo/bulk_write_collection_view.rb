# Copyright (C) 2009-2013 MongoDB, Inc.
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

  # References
  #   Fluent Interface - https://wiki.mongodb.com/display/10GEN/Fluent+Interface
  #   Bulk API Spec - https://github.com/10gen/specifications/blob/master/source/driver-bulk-update.rst

  class BulkWriteCollectionView

    DEFAULT_OP_ARGS = {:q => {}}

    attr_reader :collection, :options, :ops, :op_args

    def initialize(collection, options = {})
      @collection = collection
      @options = options
      @ops = []
      @op_args = DEFAULT_OP_ARGS.dup
    end

    def inspect
      vars = [:@options, :@ops, :@op_args]
      vars_inspect = vars.collect{|var| "#{var}=#{instance_variable_get(var).inspect}"}
      "#<Mongo::BulkWriteCollectionView:0x#{self.object_id} " <<
      "@collection=#<Mongo::Collection:0x#{@collection.object_id}>, #{vars_inspect.join(', ')}>"
    end

    def find(q)
      op_args_set(:q, q)
    end

    def upsert!(value = true)
      op_args_set(:upsert, value)
    end

    def upsert(value = true)
      dup.upsert!(value)
    end

    def update(u)
      raise MongoArgumentError, "document must start with an operator" unless update_doc?(u)
      op_push([:update, @op_args.merge(:u => u, :multi => true)])
    end

    def update_one(u)
      raise MongoArgumentError, "document must start with an operator" unless update_doc?(u)
      op_push([:update, @op_args.merge(:u => u, :multi => false)])
    end

    def replace_one(u)
      raise MongoArgumentError, "document must not contain any operators" unless replace_doc?(u)
      op_push([:update, @op_args.merge(:u => u, :multi => false)])
    end

    def remove_one
      op_push([:delete, @op_args.merge(:limit => 1)])
    end

    def remove
      op_push([:delete, @op_args.merge(:limit => 0)])
    end

    def insert(document)
      op_push([:insert, document])
    end

    def execute(options = {})
      result = []
      errors = []
      ordered_group_by_first(@ops).each do |op, documents|
        check_keys = false
        if op == :insert
          documents.collect! { |doc| @collection.pk_factory.create_pk(doc) }
          check_keys = true
        end
        begin
          result << @collection.batch_write_incremental(op, documents, check_keys,
            options.merge(:continue_on_error => !@options[:ordered], :collect_on_error => true))
        rescue => ex
          errors << ex
          break if @options[:ordered]
        end
      end
      @ops = []
      [result, errors]
    end

    private

    def initialize_copy(other)
      other.instance_variable_set(:@options, other.options.dup)
    end

    def op_args_set(op, value)
      @op_args[op] = value
      self
    end

    def op_push(op)
      @ops << op
      self
    end

    def update_doc?(doc)
      !doc.empty? && doc.keys.first.to_s =~ /^\$/
    end

    def replace_doc?(doc)
      doc.keys.all?{|key| key !~ /^\$/}
    end

    def ordered_group_by_first(pairs)
      pairs.inject([[], nil]) do |memo, pair|
        result, previous_value = memo
        current_value = pair.first
        result << [current_value, []] if previous_value != current_value
        result.last.last << pair.last
        [result, current_value]
      end.first
    end

  end

  class Collection

    def initialize_ordered_bulk_op
      BulkWriteCollectionView.new(self, :ordered => true)
    end

    def initialize_unordered_bulk_op
      BulkWriteCollectionView.new(self, :ordered => false)
    end

  end

end
