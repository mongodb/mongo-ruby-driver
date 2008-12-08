require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class InsertMessage < Message

        def initialize(db_name, collection_name, *objs)
          super(OP_INSERT)
          write_int(0)
          write_string("#{db_name}.#{collection_name}")
          objs.each { |o| write_doc(o) }
        end
      end
    end
  end
end
