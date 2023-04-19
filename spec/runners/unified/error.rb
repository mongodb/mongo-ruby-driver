# frozen_string_literal: true
# rubocop:todo all

module Unified

  class Error < StandardError

    class ResultMismatch < Error
    end

    class ErrorMismatch < Error
    end

    class UnhandledField < Error
    end

    class EntityMapOverwriteAttempt < Error
    end

    class EntityMissing < Error
    end

    class InvalidTest < Error
    end

    class UnsupportedOperation < Error
    end
  end
end
