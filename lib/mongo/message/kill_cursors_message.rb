require 'mongo/message/message'
require 'mongo/message/opcodes'

module XGen
  module Mongo
    module Driver

      class KillCursorsMessage < Message

        def initialize(*cursors)
          super(OP_KILL_CURSORS)
          write_int(0)
          write_int(cursors.length)
          cursors.each { |c| write_long c }
        end
      end
    end
  end
end

