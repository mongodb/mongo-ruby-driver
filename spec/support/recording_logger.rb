# frozen_string_literal: true
# rubocop:todo all

require 'stringio'

# A "Logger-alike" class, quacking like ::Logger, used for recording messages
# as they are written to the log
class RecordingLogger < Logger
  def initialize(*args, **kwargs)
    @buffer = StringIO.new
    super(@buffer, *args, **kwargs)
  end

  # Accesses the raw contents of the log
  #
  # @return [ String ] the raw contents of the log
  def contents
    @buffer.string
  end

  # Returns the contents of the log as individual lines.
  #
  # @return [ Array<String> ] the individual log lines
  def lines
    contents.split(/\n/)
  end
end
