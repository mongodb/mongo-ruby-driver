require 'mongo/pool/socket'

# TODO: here for now, but these should be relocated
module Mongo
  class ConnectionError < StandardError; end
  class SocketTimeoutError < StandardError; end
  class SocketError < StandardError; end
end
