require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class RemoveMessage < Message

        def initialize(name, collection, sel)
          super(OP_DELETE)
          write_int(0)
          write_string("#{name}.#{collection}")
          write_int(0)              # flags?
          write_doc(sel)
        end
      end
    end
  end
end
