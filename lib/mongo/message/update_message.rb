require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class UpdateMessage < Message

        def initialize(name, collection, sel, obj, repsert)
          super(OP_UPDATE)
          write_int(0)
          write_string("#{name}.#{collection}")
          write_int(repsert ? 1 : 0) # 1 if a repsert operation (upsert)
          write_doc(sel)
          write_doc(obj)
        end
      end
    end
  end
end
