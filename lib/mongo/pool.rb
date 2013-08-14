require 'mongo/pool/socket'
require 'mongo/pool/connection'

module Mongo
  class SocketError < StandardError; end
  class SocketTimeoutError < SocketError; end
  class ConnectionError < StandardError; end
end
