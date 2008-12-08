require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class RemoveMessage < Message

        def initialize(db_name, collection_name, sel)
          super(OP_DELETE)
          write_int(0)
          write_string("#{db_name}.#{collection_name}")
          write_int(0)              # flags?
          write_doc(sel)
        end
      end
    end
  end
end
