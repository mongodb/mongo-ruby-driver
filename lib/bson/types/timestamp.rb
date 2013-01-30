module BSON

  # A class for representing BSON Timestamps. The Timestamp type is used
  # by MongoDB internally; thus, it should be used by application developers
  # for diagnostic purposes only.
  class Timestamp
    include Enumerable

    attr_reader :seconds, :increment

    # Create a new BSON Timestamp.
    #
    # @param [Integer] seconds The number of seconds 
    # @param increment
    def initialize(seconds, increment)
      @seconds   = seconds
      @increment = increment
    end

    def to_s
      "seconds: #{seconds}, increment: #{increment}"
    end

    def ==(other)
      self.seconds == other.seconds &&
        self.increment == other.increment
    end

    # This is for backward-compatibility. Timestamps in the Ruby
    # driver used to deserialize as arrays. So we provide
    # an equivalent interface.
    #
    # @deprecated
    def [](index)
      warn "Timestamps are no longer deserialized as arrays. If you're working " +
        "with BSON Timestamp objects, see the BSON::Timestamp class. This usage will " +
        "be deprecated in Ruby Driver v2.0."
      if index == 0
        self.increment
      elsif index == 1
        self.seconds
      else
        nil
      end
    end

    # This method exists only for backward-compatibility.
    #
    # @deprecated
    def each
      i = 0
      while i < 2
        yield self[i]
        i += 1
      end
    end
  end
end
