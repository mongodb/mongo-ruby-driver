class ByteBuffer

  attr_reader :order

  def initialize(initial_data=[])
    @buf = initial_data
    @cursor = 0
    self.order = :little_endian
  end

  # +endianness+ should be :little_endian or :big_endian. Default is :little_endian
  def order=(endianness)
    @order = endianness
    @int_pack_order = endianness == :little_endian ? 'V' : 'N'
    @double_pack_order = endianness == :little_endian ? 'E' : 'G'
  end

  def rewind
    @cursor = 0
  end

  def position
    @cursor
  end

  def position=(val)
    @cursor = val
  end

  def clear
    @buf = []
    rewind
  end

  def size
    @buf.size
  end
  alias_method :length, :size

  def put(byte, offset=nil)
    @cursor = offset if offset
    @buf[@cursor] = byte
    @cursor += 1
  end

  def put_array(array, offset=nil)
    @cursor = offset if offset
    @buf[@cursor, array.length] = array
    @cursor += array.length
  end

  if RUBY_VERSION >= '1.9'
    def put_int(i, offset=nil)
      put_array([i].pack(@int_pack_order).split(//).collect{|c| c.bytes.first}, offset)
    end
  else
    def put_int(i, offset=nil)
      put_array([i].pack(@int_pack_order).split(//).collect{|c| c[0]}, offset)
    end
  end

  def put_long(i, offset=nil)
    offset = @cursor unless offset
    if @int_pack_order == 'N'
      put_int(i >> 32, offset)
      put_int(i & 0xffffffff, offset + 4)
    else
      put_int(i & 0xffffffff, offset)
      put_int(i >> 32, offset + 4)
    end
  end

  if RUBY_VERSION >= '1.9'
    def put_double(d, offset=nil)
      put_array([d].pack(@double_pack_order).split(//).collect{|c| c.bytes.first}, offset)
    end
  else
    def put_double(d, offset=nil)
      put_array([d].pack(@double_pack_order).split(//).collect{|c| c[0]}, offset)
    end
  end

  # If +size+ == 1, returns one byte. Else returns array of bytes of length
  # +size+.
  def get(len=1)
    check_read_length(len)
    start = @cursor
    @cursor += len
    if len == 1
      @buf[start]
    else
      @buf[start, len]
    end
  end

  def get_int
    check_read_length(4)
    vals = ""
    (@cursor..@cursor+3).each { |i| vals << @buf[i].chr }
    @cursor += 4
    vals.unpack(@int_pack_order)[0]
  end

  def get_long
    i1 = get_int
    i2 = get_int
    if @int_pack_order == 'N'
      (i1 << 32) + i2
    else
      (i2 << 32) + i1
    end
  end

  def get_double
    check_read_length(8)
    vals = ""
    (@cursor..@cursor+7).each { |i| vals << @buf[i].chr }
    @cursor += 8
    vals.unpack(@double_pack_order)[0]
  end

  def more?
    @cursor < @buf.size
  end

  def to_a
    @buf
  end

  def to_s
    @buf.pack("C*")
  end

  def dump
    @buf.each_with_index { |c, i| $stderr.puts "#{'%04d' % i}: #{'%02x' % c} #{'%03o' % c} #{'%s' % c.chr} #{'%3d' % c}" }
  end

  private

  def check_read_length(len)
    raise "attempt to read past end of buffer" if @cursor + len > @buf.length
  end

end
