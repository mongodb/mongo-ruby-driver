#
# = uuid.rb - UUID generator
#
# Author:: Assaf Arkin  assaf@labnotes.org
#          Eric Hodel drbrain@segment7.net
# Copyright:: Copyright (c) 2005-2008 Assaf Arkin, Eric Hodel
# License:: MIT and/or Creative Commons Attribution-ShareAlike

require 'fileutils'
require 'thread'
require 'tmpdir'

# require 'rubygems'
require 'mongo/util/macaddr'


##
# = Generating UUIDs
#
# Call #generate to generate a new UUID. The method returns a string in one of
# three formats. The default format is 36 characters long, and contains the 32
# hexadecimal octets and hyphens separating the various value parts. The
# <tt>:compact</tt> format omits the hyphens, while the <tt>:urn</tt> format
# adds the <tt>:urn:uuid</tt> prefix.
#
# For example:
#
#   uuid = UUID.new
#   
#   10.times do
#     p uuid.generate
#   end
#
# = UUIDs in Brief
#
# UUID (universally unique identifier) are guaranteed to be unique across time
# and space.
#
# A UUID is 128 bit long, and consists of a 60-bit time value, a 16-bit
# sequence number and a 48-bit node identifier.
#
# The time value is taken from the system clock, and is monotonically
# incrementing.  However, since it is possible to set the system clock
# backward, a sequence number is added.  The sequence number is incremented
# each time the UUID generator is started.  The combination guarantees that
# identifiers created on the same machine are unique with a high degree of
# probability.
#
# Note that due to the structure of the UUID and the use of sequence number,
# there is no guarantee that UUID values themselves are monotonically
# incrementing.  The UUID value cannot itself be used to sort based on order
# of creation.
#
# To guarantee that UUIDs are unique across all machines in the network,
# the IEEE 802 MAC address of the machine's network interface card is used as
# the node identifier.
#
# For more information see {RFC 4122}[http://www.ietf.org/rfc/rfc4122.txt].

class UUID

  VERSION = '2.0.1'

  ##
  # Clock multiplier. Converts Time (resolution: seconds) to UUID clock
  # (resolution: 10ns)
  CLOCK_MULTIPLIER = 10000000

  ##
  # Clock gap is the number of ticks (resolution: 10ns) between two Ruby Time
  # ticks.
  CLOCK_GAPS = 100000

  ##
  # Version number stamped into the UUID to identify it as time-based.
  VERSION_CLOCK = 0x0100

  ##
  # Formats supported by the UUID generator.
  #
  # <tt>:default</tt>:: Produces 36 characters, including hyphens separating
  #                     the UUID value parts
  # <tt>:compact</tt>:: Produces a 32 digits (hexadecimal) value with no
  #                     hyphens
  # <tt>:urn</tt>:: Adds the prefix <tt>urn:uuid:</tt> to the default format
  FORMATS = {
    :compact => '%08x%04x%04x%04x%012x',
    :default => '%08x-%04x-%04x-%04x-%012x',
    :urn     => 'urn:uuid:%08x-%04x-%04x-%04x-%012x',
  }

  ##
  # MAC address (48 bits), sequence number and last clock
  STATE_FILE_FORMAT = 'SLLQ'

  @state_file = nil
  @mode = nil
  @uuid = nil

  ##
  # The access mode of the state file.  Set it with state_file.

  def self.mode
    @mode
  end

  ##
  # Generates a new UUID string using +format+.  See FORMATS for a list of
  # supported formats.

  def self.generate(format = :default)
    @uuid ||= new
    @uuid.generate format
  end

  ##
  # Creates an empty state file in /var/tmp/ruby-uuid or the windows common
  # application data directory using mode 0644.  Call with a different mode
  # before creating a UUID generator if you want to open access beyond your
  # user by default.
  #
  # If the default state dir is not writable, UUID falls back to ~/.ruby-uuid.
  #
  # State files are not portable across machines.
  def self.state_file(mode = 0644)
    return @state_file if @state_file

    @mode = mode

    begin
      require 'Win32API'

      csidl_common_appdata = 0x0023
      path = 0.chr * 260
      get_folder_path = Win32API.new('shell32', 'SHGetFolderPath', 'LLLLP', 'L')
      get_folder_path.call 0, csidl_common_appdata, 0, 1, path

      state_dir = File.join(path.strip)
    rescue LoadError
      state_dir = File.join('', 'var', 'tmp')
    end

    if File.writable?(state_dir) then
      @state_file = File.join(state_dir, 'ruby-uuid')
    else
      @state_file = File.expand_path(File.join('~', '.ruby-uuid'))
    end

    @state_file
  end

  ##
  # Create a new UUID generator.  You really only need to do this once.
  def initialize
    @drift = 0
    @last_clock = (Time.now.to_f * CLOCK_MULTIPLIER).to_i
    @mutex = Mutex.new

    if File.exist?(self.class.state_file) then
      next_sequence
    else
      @mac = Mac.addr.gsub(/:|-/, '').hex & 0x7FFFFFFFFFFF
      fail "Cannot determine MAC address from any available interface, tried with #{Mac.addr}" if @mac == 0
      @sequence = rand 0x10000

      open_lock 'w' do |io|
        write_state io
      end
    end
  end

  ##
  # Generates a new UUID string using +format+.  See FORMATS for a list of
  # supported formats.
  def generate(format = :default)
    template = FORMATS[format]

    raise ArgumentError, "invalid UUID format #{format.inspect}" unless template

    # The clock must be monotonically increasing. The clock resolution is at
    # best 100 ns (UUID spec), but practically may be lower (on my setup,
    # around 1ms). If this method is called too fast, we don't have a
    # monotonically increasing clock, so the solution is to just wait.
    #
    # It is possible for the clock to be adjusted backwards, in which case we
    # would end up blocking for a long time. When backward clock is detected,
    # we prevent duplicates by asking for a new sequence number and continue
    # with the new clock.

    clock = @mutex.synchronize do
      clock = (Time.new.to_f * CLOCK_MULTIPLIER).to_i & 0xFFFFFFFFFFFFFFF0

      if clock > @last_clock then
        @drift = 0
        @last_clock = clock
      elsif clock == @last_clock then
        drift = @drift += 1

        if drift < 10000 then
          @last_clock += 1
        else
          Thread.pass
          nil
        end
      else
        next_sequence
        @last_clock = clock
      end
    end until clock

    template % [
        clock        & 0xFFFFFFFF,
       (clock >> 32) & 0xFFFF,
      ((clock >> 48) & 0xFFFF | VERSION_CLOCK),
      @sequence      & 0xFFFF,
      @mac           & 0xFFFFFFFFFFFF
    ]
  end

  ##
  # Updates the state file with a new sequence number.
  def next_sequence
    open_lock 'r+' do |io|
      @mac, @sequence, @last_clock = read_state(io)

      io.rewind
      io.truncate 0

      @sequence += 1

      write_state io
    end
  rescue Errno::ENOENT
    open_lock 'w' do |io|
      write_state io
    end
  ensure
    @last_clock = (Time.now.to_f * CLOCK_MULTIPLIER).to_i
    @drift = 0
  end

  def inspect
    mac = ("%012x" % @mac).scan(/[0-9a-f]{2}/).join(':')
    "MAC: #{mac}  Sequence: #{@sequence}"
  end

protected

  ##
  # Open the state file with an exclusive lock and access mode +mode+.
  def open_lock(mode)
    File.open self.class.state_file, mode, self.class.mode do |io|
      begin
        io.flock File::LOCK_EX
        yield io
      ensure
        io.flock File::LOCK_UN
      end
    end
  end

  ##
  # Read the state from +io+
  def read_state(io)
    mac1, mac2, seq, last_clock = io.read(32).unpack(STATE_FILE_FORMAT)
    mac = (mac1 << 32) + mac2

    return mac, seq, last_clock
  end


  ##
  # Write that state to +io+
  def write_state(io)
    mac2 =  @mac        & 0xffffffff
    mac1 = (@mac >> 32) & 0xffff

    io.write [mac1, mac2, @sequence, @last_clock].pack(STATE_FILE_FORMAT)
  end
  
end
