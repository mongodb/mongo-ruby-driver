# frozen_string_literal: true
# rubocop:todo all

module Unified

  class Error < StandardError
  end

  class ResultMismatch < Error
  end

  class ErrorMismatch < Error
  end

  class EntityMapOverwriteAttempt < Error
  end

  class EntityMissing < Error
  end

  class InvalidTest < Error
  end

end
