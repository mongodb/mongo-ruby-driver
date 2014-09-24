require 'cucumber/formatter/pretty'

module Cucumber
  module Formatter
    class Pretty
      def comment_line(comment_line)
        return if comment_line =~ /^#/
        @io.puts(comment_line.indent(@indent))
        @io.flush
      end
    end
  end
end
