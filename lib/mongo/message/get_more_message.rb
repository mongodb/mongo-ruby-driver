require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class GetMoreMessage < Message

        def initialize(db_name, collection_name, cursor)
          super(OP_GET_MORE)
          write_int(0)
          write_string("#{db_name}.#{collection_name}")
          write_int(0)              # num to return; leave it up to the db for now
          write_long(cursor)
        end
      end
    end
  end
end

