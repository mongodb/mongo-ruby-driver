require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class MsgMessage < Message

        def initialize(msg)
          super(OP_MSG)
          write_string(msg)
        end
      end
    end
  end
end
