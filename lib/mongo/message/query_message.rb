require 'mongo/message/message'
require 'mongo/message/opcodes'
require 'mongo/util/ordered_hash'

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
          sel = query.selector
          if query.order_by
            sel = OrderedHash.new
            sel['query'] = query.selector
            sel['orderby'] = case query.order_by
                             when Array
                               if query.order_by.empty? # Empty array of order_by values 
                                []
                               else
                                 case query.order_by[0]
                                 when Hash # Array of hashes
                                   query.order_by
                                 else      # ['a', 'b']
                                   query.order_by.collect { |v| {v => 1} } # Assume ascending order for all values
                                 end
                               end
                             when Hash # Should be an ordered hash, but this message doesn't care
                               a = []
                               query.order_by.each { |k,v| a << {k => v }}
                               a
                             end
                               
          end
          write_doc(sel)
          write_doc(query.fields) if query.fields
        end

        def first_key(key)
          @first_key = key
        end
      end
    end
  end
end
