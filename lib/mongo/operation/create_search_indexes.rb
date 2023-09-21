# frozen_string_literal: true

require 'mongo/operation/create_search_indexes/op_msg'

module Mongo
  module Operation
    # A MongoDB createSearchIndexes command operation.
    #
    # @api private
    class CreateSearchIndexes
      include Specifiable
      include OpMsgExecutable
    end
  end
end
