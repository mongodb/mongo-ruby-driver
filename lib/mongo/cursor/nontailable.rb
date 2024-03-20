# frozen_string_literal: true

module Mongo
  class Cursor
    # This module is used by cursor-implementing classes to indicate that
    # the only cursors they generate are non-tailable, and iterable.
    #
    # @api private
    module NonTailable
      # These views are always non-tailable.
      #
      # @return [ nil ] indicating a non-tailable cursor.
      def cursor_type
        nil
      end

      # These views apply timeouts to each iteration of a cursor, as
      # opposed to the entire lifetime of the cursor.
      #
      # @return [ :iterable ] indicating a cursor with a timeout mode of
      #   "iterable".
      def timeout_mode
        :iterable
      end
    end
  end
end
