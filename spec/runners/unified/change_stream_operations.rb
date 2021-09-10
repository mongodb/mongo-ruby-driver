# frozen_string_literal: true
# encoding: utf-8

module Unified

  module ChangeStreamOperations

    def create_change_stream(op)
      object_id = op.use!('object')
      object = entities.get_any(object_id)
      use_arguments(op) do |args|
        pipeline = args.use!('pipeline')
        opts = {}
        if batch_size = args.use('batchSize')
          opts[:batch_size] = batch_size
        end
        cs = object.watch(pipeline, **opts)
        name = op.use!('saveResultAsEntity')
        entities.set(:change_stream, name, cs)
      end
    end

    def iterate_until_document_or_error(op)
      object_id = op.use!('object')
      object = entities.get(:change_stream, object_id)
      object.to_enum.next
    end
  end
end
