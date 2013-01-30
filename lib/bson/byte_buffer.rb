# A byte buffer.
module BSON
  class ByteBuffer

    attr_reader :order, :max_size

    INT32_PACK = 'l<'.freeze
    INT64_PACK = 'q<'.freeze
    DOUBLE_PACK = 'E'.freeze

    def initialize(initial_data="", max_size=DEFAULT_MAX_BSON_SIZE)
      @str = case initial_data
        when String then
          if initial_data.respond_to?(:force_encoding)
            initial_data.force_encoding('binary')
          else
            initial_data
          end
        when BSON::ByteBuffer then
          initial_data.to_a.pack('C*')
        else
          initial_data.pack('C*')
      end

      @cursor = @str.length
      @max_size = max_size
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
      @str = ""
      @str.force_encoding('binary') if @str.respond_to?(:force_encoding)
      rewind
    end

    def size
      @str.size
    end
    alias_method :length, :size

    # Appends a second ByteBuffer object, +buffer+, to the current buffer.
    def append!(buffer)
      @str << buffer.to_s
      self
    end

    # Prepends a second ByteBuffer object, +buffer+, to the current buffer.
    def prepend!(buffer)
      @str = buffer.to_s + @str
      self
    end

    def put(byte, offset=nil)
      @cursor = offset if offset
      if more?
        @str[@cursor] = chr(byte)
      else
        ensure_length(@cursor)
        @str << chr(byte)
      end
      @cursor += 1
    end
    
    def put_binary(data, offset=nil)
      @cursor = offset if offset
      if defined?(BINARY_ENCODING)
        data = data.dup.force_encoding(BINARY_ENCODING)
      end
      if more?
        @str[@cursor, data.length] = data
      else
        ensure_length(@cursor)
        @str << data
      end
      @cursor += data.length
    end
    
    def put_array(array, offset=nil)
      @cursor = offset if offset
      if more?
        @str[@cursor, array.length] = array.pack("C*")
      else
        ensure_length(@cursor)
        @str << array.pack("C*")
      end
      @cursor += array.length
    end

    def put_num(i, offset, bytes)
      pack_type = bytes == 4 ? INT32_PACK : INT64_PACK
      @cursor = offset if offset
      if more?
        @str[@cursor, bytes] = [i].pack(pack_type)
      else
        ensure_length(@cursor)
        @str << [i].pack(pack_type)
      end
      @cursor += bytes
    end

    def put_int(i, offset=nil)
      put_num(i, offset, 4)
    end

    def put_long(i, offset=nil)
      put_num(i, offset, 8)
    end

    def put_double(d, offset=nil)
      a = []
      [d].pack(DOUBLE_PACK).each_byte { |b| a << b }
      put_array(a, offset)
    end

    # If +size+ == nil, returns one byte. Else returns array of bytes of length
    # # +size+.
    if "x"[0].is_a?(Integer)
      def get(len=nil)
        one_byte = len.nil?
        len ||= 1
        check_read_length(len)
        start = @cursor
        @cursor += len
        if one_byte
          @str[start]
        else
          @str[start, len].unpack("C*")
        end
      end
    else
      def get(len=nil)
        one_byte = len.nil?
        len ||= 1
        check_read_length(len)
        start = @cursor
        @cursor += len
        if one_byte
          @str[start, 1].ord
        else
          @str[start, len].unpack("C*")
        end
      end
    end

    def get_int
      check_read_length(4)
      vals = @str[@cursor..@cursor+3]
      @cursor += 4
      vals.unpack(INT32_PACK)[0]
    end

    def get_long
      check_read_length(8)
      vals = @str[@cursor..@cursor+7]
      @cursor += 8
      vals.unpack(INT64_PACK)[0]
    end

    def get_double
      check_read_length(8)
      vals = @str[@cursor..@cursor+7]
      @cursor += 8
      vals.unpack(DOUBLE_PACK)[0]
    end

    def more?
      @cursor < @str.size
    end
    
    def ==(other)
      other.respond_to?(:to_s) && @str == other.to_s
    end

    def to_a(format="C*")
      @str.unpack(format)
    end

    def unpack(format="C*")
      to_a(format)
    end

    def to_s
      @str
    end

    def dump
      @str.each_byte do |c, i|
        $stderr.puts "#{'%04d' % i}: #{'%02x' % c} #{'%03o' % c} #{'%s' % c.chr} #{'%3d' % c}"
        i += 1
      end
    end

    private

    def ensure_length(length)
      if @str.size < length
        @str << NULL_BYTE * (length - @str.size)
      end
    end
    
    def chr(byte)
      if byte < 0
        [byte].pack('c')
      else
        byte.chr
      end
    end
    
    def check_read_length(len)
      raise "attempt to read past end of buffer" if @cursor + len > @str.length
    end

  end
end
