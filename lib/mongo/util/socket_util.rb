module SocketUtil

  attr_accessor :pool, :pid

  def checkout
    @pool.checkout if @pool
  end

  def checkin
    @pool.checkin(self) if @pool
  end

  def close
    @socket.close unless closed?
  end

  def closed?
    @socket.closed?
  end
end
