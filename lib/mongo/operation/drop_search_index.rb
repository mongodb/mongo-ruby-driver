# frozen_string_literal: true

require 'mongo/operation/drop_search_index/op_msg'

module Mongo
  module Operation
    # A MongoDB dropSearchIndex command operation.
    #
    # @api private
    class DropSearchIndex
      include Specifiable
      include OpMsgExecutable
    end
  end
end
