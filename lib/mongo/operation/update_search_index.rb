# frozen_string_literal: true

require 'mongo/operation/update_search_index/op_msg'

module Mongo
  module Operation
    # A MongoDB updateSearchIndex command operation.
    #
    # @api private
    class UpdateSearchIndex
      include Specifiable
      include OpMsgExecutable
    end
  end
end
