require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class QueryMessage < Message

        def initialize(db_name, collection_name, query)
          super(OP_QUERY)
          write_int(0)
          write_string("#{db_name}.#{collection_name}")
          write_int(query.number_to_skip)
          write_int(query.number_to_return)
          write_doc(query.selector)
          write_doc(query.fields) if query.fields
        end

        def first_key(key)
          @first_key = key
        end
      end
    end
  end
end
