module Mongo
  class Node
    attr_accessor :host, :port, :address
  
    def initialize(data)
      data = data.split(':') if data.is_a?(String)
      self.host = data[0]
      self.port = data[1] ? data[1].to_i : Connection::DEFAULT_PORT
      self.address = "#{host}:#{port}"
    end
    def eql?(other)
      other.is_a?(Node) && host == other.host && port == other.port
    end
    alias :== :eql?
    def hash
      address.hash
    end
    def <=>(other)
      address <=> other.address
    end
  end
end