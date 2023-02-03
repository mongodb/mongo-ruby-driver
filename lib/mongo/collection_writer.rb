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

  class CollectionWriter
    include Mongo::Logging
    include Mongo::WriteConcern

    OPCODE = {
      :insert => Mongo::Constants::OP_INSERT,
      :update => Mongo::Constants::OP_UPDATE,
      :delete => Mongo::Constants::OP_DELETE
    }
    WRITE_COMMAND_ARG_KEY = {
      :insert => :documents,
      :update => :updates,
      :delete => :deletes
    }

    def initialize(collection)
      @collection = collection
      @name = @collection.name
      @db = @collection.db
      @connection = @db.connection
      @logger     = @connection.logger
      @max_write_batch_size = Mongo::MongoClient::DEFAULT_MAX_WRITE_BATCH_SIZE
    end

    # common implementation only for new batch write commands (insert, update, delete) and old batch insert
    def batch_write_incremental(op_type, documents, check_keys=true, opts={})
      raise Mongo::OperationFailure, "Request contains no documents" if documents.empty?
      write_concern = get_write_concern(opts, @collection)
      max_message_size, max_append_size, max_serialize_size = batch_write_max_sizes(write_concern)
      ordered = opts[:ordered]
      continue_on_error = !!opts[:continue_on_error] || ordered == false
      collect_on_error = !!opts[:collect_on_error] || ordered == false
      error_docs = [] # docs with serialization errors
      errors = []
      write_concern_errors = []
      exchanges = []
      serialized_doc = nil
      message = BSON::ByteBuffer.new("", max_message_size)
      @max_write_batch_size = @collection.db.connection.max_write_batch_size
      docs = documents.dup
      catch(:error) do
        until docs.empty? || (!errors.empty? && !collect_on_error && !continue_on_error) # process documents a batch at a time
          batch_docs = []
          batch_message_initialize(message, op_type, continue_on_error, write_concern)
          while !docs.empty? && batch_docs.size < @max_write_batch_size
            begin
              doc = docs.first
              doc = doc[:d] if op_type == :insert && !ordered.nil? #check_keys for :update outside of serialize
              serialized_doc ||= BSON::BSON_CODER.serialize(doc, check_keys, true, max_serialize_size)
            rescue BSON::InvalidDocument, BSON::InvalidKeyName, BSON::InvalidStringEncoding => ex
              bulk_message = "Bulk write error - #{ex.message} - examine result for complete information"
              ex = BulkWriteError.new(bulk_message, Mongo::ErrorCode::INVALID_BSON,
                                      {:op_type => op_type, :serialize => doc, :ord => docs.first[:ord], :error => ex}) unless ordered.nil?
              error_docs << docs.shift
              errors << ex
              next if collect_on_error
              throw(:error) if batch_docs.empty?
              break # defer exit and send batch
            end
            break if message.size + serialized_doc.size > max_append_size
            batch_docs << docs.shift
            batch_message_append(message, serialized_doc, write_concern)
            serialized_doc = nil
          end
          begin
            response = batch_message_send(message, op_type, batch_docs, write_concern, continue_on_error) if batch_docs.size > 0
            exchanges << {:op_type => op_type, :batch => batch_docs, :opts => opts, :response => response}
          rescue Mongo::WriteConcernError => ex
            write_concern_errors << ex
            exchanges << {:op_type => op_type, :batch => batch_docs, :opts => opts, :response => ex.result}
          rescue Mongo::OperationFailure => ex
            errors << ex
            exchanges << {:op_type => op_type, :batch => batch_docs, :opts => opts, :response => ex.result}
            throw(:error) unless continue_on_error
          end
        end
      end
      [error_docs, errors, write_concern_errors, exchanges]
    end

    def batch_write_partition(op_type, documents, check_keys, opts)
      raise Mongo::OperationFailure, "Request contains no documents" if documents.empty?
      write_concern = get_write_concern(opts, @collection)
      ordered = opts[:ordered]
      continue_on_error = !!opts[:continue_on_error] || ordered == false # continue_on_error default false
      collect_on_error = !!opts[:collect_on_error] # collect_on_error default false
      error_docs = [] # docs with serialization errors
      errors = []
      write_concern_errors = []
      exchanges = []
      @max_write_batch_size = @collection.db.connection.max_write_batch_size
      @write_batch_size = [documents.size, @max_write_batch_size].min
      docs = documents.dup
      until docs.empty?
        batch = docs.take(@write_batch_size)
        begin
          batch_to_send = batch #(op_type == :insert && !ordered.nil?) ? batch.collect{|doc|doc[:d]} : batch
          if @collection.db.connection.use_write_command?(write_concern) # TODO - polymorphic send_write including legacy insert
            response = send_bulk_write_command(op_type, batch_to_send, check_keys, opts)
          else
            response = send_write_operation(op_type, nil, batch_to_send, check_keys, opts, write_concern)
          end
          exchanges << {:op_type => op_type, :batch => batch, :opts => opts, :response => response}
          docs = docs.drop(batch.size)
          @write_batch_size = [(@write_batch_size*1097) >> 10, @write_batch_size+1].max unless docs.empty? # 2**(1/10) multiplicative increase
          @write_batch_size = @max_write_batch_size if @write_batch_size > @max_write_batch_size
        rescue BSON::InvalidDocument, BSON::InvalidKeyName, BSON::InvalidStringEncoding => ex
          if @write_batch_size > 1 # decrease batch size
            @write_batch_size = (@write_batch_size+1) >> 1 # 2**(-1) multiplicative decrease
            next
          end
          # error on a single document
          bulk_message = "Bulk write error - #{ex.message} - examine result for complete information"
          ex = BulkWriteError.new(bulk_message, Mongo::ErrorCode::INVALID_BSON,
                                  {:op_type => op_type, :batch => batch, :ord => batch.first[:ord], :opts => opts, :error => ex}) unless ordered.nil?
          error_docs << docs.shift
          next if collect_on_error
          errors << ex
          break unless continue_on_error
        rescue Mongo::WriteConcernError => ex
          write_concern_errors << ex
          exchanges << {:op_type => op_type, :batch => batch_docs, :opts => opts, :response => ex.result}
          docs = docs.drop(batch.size)
        rescue Mongo::OperationFailure => ex
          errors << ex
          exchanges << {:op_type => op_type, :batch => batch, :opts => opts, :response => ex.result}
          docs = docs.drop(batch.size)
          break if !continue_on_error && !collect_on_error
        end
      end
      [error_docs, errors, write_concern_errors, exchanges]
    end

    alias :batch_write :batch_write_incremental

    def send_bulk_write_command(op_type, documents, check_keys, opts, collection_name=@name)
      if op_type == :insert
        documents = documents.collect{|doc| doc[:d]} if opts.key?(:ordered)
        documents.each do |doc|
          # TODO - @pk_factory.create_pk(doc)
          if check_keys
            doc.each_key do |key|
              key = key.to_s
              raise BSON::InvalidKeyName.new("key #{key} must not start with '$'") if key[0] == ?$
              raise BSON::InvalidKeyName.new("key #{key} must not contain '.'") if key.include? ?.
            end
          end
        end
      #elsif op_type == :update # TODO - check keys
      #elsif op_type == :delete
      #else
      #  raise ArgumentError, "Write operation type must be :insert, :update or :delete"
      end
      request = BSON::OrderedHash[op_type, collection_name].merge!(
          Mongo::CollectionWriter::WRITE_COMMAND_ARG_KEY[op_type] => documents,
          :writeConcern => get_write_concern(opts, @collection),
          :ordered => opts[:ordered] || !opts[:continue_on_error]
      )
      @db.command(request)
    end

    private

    def sort_by_first_sym(pairs)
      pairs = pairs.collect{|first, rest| [first.to_s, rest]} #stringify_first
      pairs = pairs.sort{|x,y| x.first <=> y.first }
      pairs.collect{|first, rest| [first.to_sym, rest]} #symbolize_first
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

  class CollectionOperationWriter < CollectionWriter
    def initialize(collection)
      super(collection)
    end

    def send_write_operation(op_type, selector, doc_or_docs, check_keys, opts, write_concern, collection_name=@name)
      message = BSON::ByteBuffer.new("", @connection.max_message_size)
      message.put_int((op_type == :insert && !!opts[:continue_on_error]) ? 1 : 0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{collection_name}")
      if op_type == :update
        update_options  = 0
        update_options += 1 if opts[:upsert]
        update_options += 2 if opts[:multi]
        message.put_int(update_options)
      elsif op_type == :delete
        delete_options = 0
        delete_options += 1 if opts[:limit] && opts[:limit] != 0
        message.put_int(delete_options)
      end
      message.put_binary(BSON::BSON_CODER.serialize(selector, false, true, @connection.max_bson_size).to_s) if selector
      [doc_or_docs].flatten(1).compact.each do |document|
        message.put_binary(BSON::BSON_CODER.serialize(document, check_keys, true, @connection.max_bson_size).to_s)
        if message.size > @connection.max_message_size
          raise BSON::InvalidDocument, "Message is too large. This message is limited to #{@connection.max_message_size} bytes."
        end
      end
      instrument(op_type, :database => @db.name, :collection => collection_name, :selector => selector, :documents => doc_or_docs) do
        op_code = OPCODE[op_type]
        if Mongo::WriteConcern.gle?(write_concern)
          @connection.send_message_with_gle(op_code, message, @db.name, nil, write_concern)
        else
          @connection.send_message(op_code, message)
        end
      end
    end

    def bulk_execute(ops, options, opts = {})
      write_concern = get_write_concern(opts, @collection)
      errors = []
      write_concern_errors = []
      exchanges = []
      ops.each do |op_type, doc|
        doc = {:d => @collection.pk_factory.create_pk(doc[:d]), :ord => doc[:ord]} if op_type == :insert
        doc_opts = doc.merge(opts)
        d = doc_opts.delete(:d)
        q = doc_opts.delete(:q)
        u = doc_opts.delete(:u)
        begin  # use single and NOT batch inserts since there no index for an error
          response = @collection.operation_writer.send_write_operation(op_type, q, d || u, check_keys = false, doc_opts, write_concern)
          exchanges << {:op_type => op_type, :batch => [doc], :opts => opts, :response => response}
        rescue BSON::InvalidDocument, BSON::InvalidKeyName, BSON::InvalidStringEncoding => ex
          bulk_message = "Bulk write error - #{ex.message} - examine result for complete information"
          ex = BulkWriteError.new(bulk_message, Mongo::ErrorCode::INVALID_BSON,
                                  {:op_type => op_type, :serialize => doc, :ord => doc[:ord], :error => ex})
          errors << ex
          break if options[:ordered]
        rescue Mongo::WriteConcernError => ex
          write_concern_errors << ex
          exchanges << {:op_type => op_type, :batch => [doc], :opts => opts, :response => ex.result}
        rescue Mongo::OperationFailure => ex
          errors << ex
          exchanges << {:op_type => op_type, :batch => [doc], :opts => opts, :response => ex.result}
          break if options[:ordered] && ex.result["err"] != "norepl"
        end
      end
      [errors, write_concern_errors, exchanges]
    end

    private

    def batch_message_initialize(message, op_type, continue_on_error, write_concern)
      message.clear!.clear
      message.put_int(continue_on_error ? 1 : 0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@name}")
    end

    def batch_message_append(message, serialized_doc, write_concern)
      message.put_binary(serialized_doc.to_s)
    end

    def batch_message_send(message, op_type, batch_docs, write_concern, continue_on_error)
      instrument(:insert, :database => @db.name, :collection => @name, :documents => batch_docs) do
        if Mongo::WriteConcern.gle?(write_concern)
          @connection.send_message_with_gle(Mongo::Constants::OP_INSERT, message, @db.name, nil, write_concern)
        else
          @connection.send_message(Mongo::Constants::OP_INSERT, message)
        end
      end
    end

    def batch_write_max_sizes(write_concern)
      [@connection.max_message_size, @connection.max_message_size, @connection.max_bson_size]
    end

  end

  class CollectionCommandWriter < CollectionWriter
    def initialize(collection)
      super(collection)
    end

    def send_write_command(op_type, selector, doc_or_docs, check_keys, opts, write_concern, collection_name=@name)
      if op_type == :insert
        argument = [doc_or_docs].flatten(1).compact
      elsif op_type == :update
        argument = [{:q => selector, :u => doc_or_docs, :multi => !!opts[:multi]}]
        argument.first.merge!(:upsert => opts[:upsert]) if opts[:upsert]
      elsif op_type == :delete
        argument = [{:q => selector, :limit => (opts[:limit] || 0)}]
      else
        raise ArgumentError, "Write operation type must be :insert, :update or :delete"
      end
      request = BSON::OrderedHash[op_type, collection_name, WRITE_COMMAND_ARG_KEY[op_type], argument]
      request.merge!(:writeConcern => write_concern, :ordered => !opts[:continue_on_error])
      # request.merge!(opts)
      instrument(op_type, :database => @db.name, :collection => collection_name, :selector => selector, :documents => doc_or_docs) do
        @db.command(request)
      end
    end

    def bulk_execute(ops, options, opts = {})
      errors = []
      write_concern_errors = []
      exchanges = []
      ops = (options[:ordered] == false) ? sort_by_first_sym(ops) : ops # sort by write-type
      ordered_group_by_first(ops).each do |op_type, documents|
        documents.collect! {|doc| {:d => @collection.pk_factory.create_pk(doc[:d]), :ord => doc[:ord]} } if op_type == :insert
        error_docs, batch_errors, batch_write_concern_errors, batch_exchanges =
            batch_write(op_type, documents, check_keys = false, opts.merge(:ordered => options[:ordered]))
        errors += batch_errors
        write_concern_errors += batch_write_concern_errors
        exchanges += batch_exchanges
        break if options[:ordered] && !batch_errors.empty?
      end
      [errors, write_concern_errors, exchanges]
    end

    private

    def batch_message_initialize(message, op_type, continue_on_error, write_concern)
      message.clear!.clear
      @bson_empty ||= BSON::BSON_CODER.serialize({})
      message.put_binary(@bson_empty.to_s)
      message.unfinish!.array!(WRITE_COMMAND_ARG_KEY[op_type])
    end

    def batch_message_append(message, serialized_doc, write_concern)
      message.push_doc!(serialized_doc)
    end

    def batch_message_send(message, op_type, batch_docs, write_concern, continue_on_error)
      message.finish!
      request = BSON::OrderedHash[op_type, @name, :bson, message]
      request.merge!(:writeConcern => write_concern, :ordered => !continue_on_error)
      instrument(:insert, :database => @db.name, :collection => @name, :documents => batch_docs) do
        @db.command(request)
      end
    end

    def batch_write_max_sizes(write_concern)
      [MongoClient::COMMAND_HEADROOM, MongoClient::APPEND_HEADROOM, MongoClient::SERIALIZE_HEADROOM].collect{|h| @connection.max_bson_size + h}
    end

  end

end

