require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class GetMoreMessage < Message

        def initialize(name, collection, cursor)
          super(OP_GET_MORE)
          write_int(0)
          write_string("#{name}.#{collection}")
          write_int(0)              # num to return; leave it up to the db for now
          write_long(cursor)
        end
      end
    end
  end
end

