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
    COMMAND_HEADROOM   = 16_384
    APPEND_HEADROOM    = COMMAND_HEADROOM / 2
    SERIALIZE_HEADROOM = APPEND_HEADROOM / 2
    MAX_WRITE_BATCH_SIZE     = 10_000

    def initialize(collection)
      @collection = collection
      @name = @collection.name
      @db = @collection.db
      @connection = @db.connection
      @logger     = @connection.logger
      @max_write_batch_size = MAX_WRITE_BATCH_SIZE
    end

    # common implementation only for new batch write commands (insert, update, delete) and old batch insert
    def batch_write_incremental(op, documents, check_keys=true, opts={})
      raise Mongo::OperationFailure, "Request contains no documents" if documents.empty?
      write_concern = get_write_concern(opts, @collection)
      max_message_size, max_append_size, max_serialize_size = batch_write_max_sizes(write_concern)
      ordered = opts[:ordered]
      continue_on_error = !!opts[:continue_on_error]
      collect_on_error = !!opts[:collect_on_error]
      error_docs = [] # docs with serialization errors
      errors = [] # for all db errors
      responses = []
      serialized_doc = nil
      message = BSON::ByteBuffer.new("", max_message_size)
      docs = documents.dup
      until docs.empty? # process documents a batch at a time
        batch_docs = []
        batch_message_initialize(message, op, continue_on_error, write_concern)
        while !docs.empty? && batch_docs.size < @max_write_batch_size
          begin
            serialized_doc ||= BSON::BSON_CODER.serialize(docs.first, check_keys, true, max_serialize_size)
          rescue BSON::InvalidDocument, BSON::InvalidKeyName, BSON::InvalidStringEncoding => ex
            raise ex unless collect_on_error
            error_docs << docs.shift
            next
          end
          break if message.size + serialized_doc.size > max_append_size
          batch_docs << docs.shift
          batch_message_append(message, serialized_doc, write_concern)
          serialized_doc = nil
        end
        begin
          responses << batch_message_send(message, op, batch_docs, write_concern, continue_on_error) if batch_docs.size > 0
        rescue OperationFailure => ex
          raise ex unless continue_on_error
          errors << ex
        end
      end
      unless ordered.nil?
        return responses if errors.empty?
        bulk_message = "Bulk write failed - #{errors.last.message} - examine result for complete information"
        raise BulkWriteError.new(bulk_message, 65, {"results" => responses, "errors" => errors})
      end
      [error_docs, responses, errors]
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

    private

    def batch_message_initialize(message, op, continue_on_error, write_concern)
      message.clear!.clear
      message.put_int(continue_on_error ? 1 : 0)
      BSON::BSON_RUBY.serialize_cstr(message, "#{@db.name}.#{@name}")
    end

    def batch_message_append(message, serialized_doc, write_concern)
      message.put_binary(serialized_doc.to_s)
    end

    def batch_message_send(message, op, batch_docs, write_concern, continue_on_error)
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
      request.merge!(opts)
      instrument(op_type, :database => @db.name, :collection => collection_name, :selector => selector, :documents => doc_or_docs) do
        @db.command(request)
      end
    end

    private

    def batch_message_initialize(message, op, continue_on_error, write_concern)
      message.clear!.clear
      @bson_empty ||= BSON::BSON_CODER.serialize({})
      message.put_binary(@bson_empty.to_s)
      message.unfinish!.array!(WRITE_COMMAND_ARG_KEY[op])
    end

    def batch_message_append(message, serialized_doc, write_concern)
      message.push_doc!(serialized_doc)
    end

    def batch_message_send(message, op, batch_docs, write_concern, continue_on_error)
      message.finish!
      request = BSON::OrderedHash[op, @name, :bson, message]
      request.merge!(:writeConcern => write_concern, :ordered => !continue_on_error)
      instrument(:insert, :database => @db.name, :collection => @name, :documents => batch_docs) do
        @db.command(request)
      end
    end

    def batch_write_max_sizes(write_concern)
      [COMMAND_HEADROOM, APPEND_HEADROOM, SERIALIZE_HEADROOM].collect{|h| @connection.max_bson_size + h}
    end

  end

end

