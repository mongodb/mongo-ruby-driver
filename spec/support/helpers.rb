module Helpers

  # Helper method to allow temporary redirection of $stdout.
  #
  # @example
  # silence do
  #   # your noisey code here
  # end
  #
  # @param A code block to execute.
  # @return Original $stdout value.
  def silence &block
    original_stdout = $stdout
    original_stderr = $stderr
    $stdout = $stderr = File.new('/dev/null', 'w')
    yield block
  ensure
    $stdout = original_stdout
    $stderr = original_stderr
  end

end
