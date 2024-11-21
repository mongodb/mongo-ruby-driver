# frozen_string_literal: true
# rubocop:todo all

module Unified

  module ChangeStreamOperations

    def create_change_stream(op)
      object_id = op.use!('object')
      object = entities.get_any(object_id)
      use_arguments(op) do |args|
        pipeline = args.use!('pipeline')
        opts = extract_options(args, 'batchSize', 'comment', 'fullDocument',
          'fullDocumentBeforeChange', 'showExpandedEvents', 'timeoutMS',
          'maxAwaitTimeMS')
        cs = object.watch(pipeline, **opts)
        if name = op.use('saveResultAsEntity')
          entities.set(:change_stream, name, cs)
        end
      end
    end

    def iterate_until_document_or_error(op)
      object_id = op.use!('object')
      object = entities.get_any(object_id)
      object.try_next
    end

    def iterate_once(op)
      stream_id = op.use!('object')
      stream = entities.get_any(stream_id)
      stream.try_next
    end

    def close(op)
      object_id = op.use!('object')
      opts = op.key?('arguments') ? extract_options(op.use!('arguments'), 'timeoutMS') : {}
      object = entities.get_any(object_id)
      object.close(opts)
    end
  end
end
