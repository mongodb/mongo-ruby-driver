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
  def silence(&block)
    original_stdout = $stdout
    original_stderr = $stderr
    $stdout = $stderr = File.new('/dev/null', 'w')
    yield block
  ensure
    $stdout = original_stdout
    $stderr = original_stderr
  end

  def node(state, opts = {})
    tags = opts.fetch(:tags, {})
    ping = opts.fetch(:ping, 0)

    double(state.to_s.capitalize).tap do |node|
      allow(node).to receive(:primary?) do
        state == :primary ? true : false
      end
      allow(node).to receive(:secondary?) do
        state == :secondary ? true : false
      end
      allow(node).to receive(:tags) { tags }
      allow(node).to receive(:matches_tags?) do |tag_set|
        tag_set.none? { |k, v| node.tags[k.to_s] != v }
      end
      allow(node).to receive(:ping_time) { ping }
    end
  end
end
