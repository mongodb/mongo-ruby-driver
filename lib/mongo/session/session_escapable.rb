
module Mongo

  class Session

    class << self
      def escape
        old_escaped = self.escaped?
        self.escaped = true
        yield
      ensure
        self.escaped = old_escaped
      end

      def escaped?
        !!Thread.current["[mongo]:session:escaped"]
      end

      private

      def escaped=(value)
        Thread.current["[mongo]:session:escaped"] = !!value
      end
    end
  end
end
