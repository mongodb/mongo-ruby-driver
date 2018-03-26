module Mongo
  class Error
    class FailedStringPrepOperation < Error
      INVALID_BIDIRECTIONAL = 'StringPrep bidirectional data is invalid'.freeze
      PROHIBITED_CHARACTER = 'StringPrep data contains a prohibited character.'.freeze

      def initialize(msg)
        super(msg)
      end
    end
  end
end
