module XGen
  module Mongo
    module Driver
      OP_REPLY = 1              # reply. responseTo is set.
      OP_MSG = 1000             # generic msg command followed by a string
      OP_UPDATE = 2001          # update object
      OP_INSERT = 2002
      # GET_BY_OID = 2003
      OP_QUERY = 2004
      OP_GET_MORE = 2005
      OP_DELETE = 2006
      OP_KILL_CURSORS = 2007
    end
  end
end

