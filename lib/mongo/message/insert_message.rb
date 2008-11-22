require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class InsertMessage < Message

        def initialize(name, collection, *objs)
          super(OP_INSERT)
          write_int(0)
          write_string("#{name}.#{collection}")
          objs.each { |o| write_doc(o) }
        end
      end
    end
  end
end

