require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class QueryMessage < Message

        def initialize(name, collection, query)
          super(OP_QUERY)
          write_int(0)
          write_string("#{name}.#{collection}")
          write_int(query.number_to_skip)
          write_int(query.number_to_return)
          write_doc(query.selector)
          write_doc(query.fields) if query.fields
        end
      end
    end
  end
end
