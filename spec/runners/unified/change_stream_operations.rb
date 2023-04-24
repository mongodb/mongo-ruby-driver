# frozen_string_literal: true
# rubocop:todo all

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
        if comment = args.use('comment')
          opts[:comment] = comment
        end
        if full_document = args.use('fullDocument')
          opts[:full_document] = full_document
        end
        if full_document_before_change = args.use('fullDocumentBeforeChange')
          opts[:full_document_before_change] = full_document_before_change
        end
        if args.key?('showExpandedEvents')
          opts[:show_expanded_events] = args.use!('showExpandedEvents')
        end
        cs = object.watch(pipeline, **opts)
        if name = op.use('saveResultAsEntity')
          entities.set(:change_stream, name, cs)
        end
      end
    end

    def iterate_until_document_or_error(op)
      object_id = op.use!('object')
      object = entities.get(:change_stream, object_id)
      object.to_enum.next
    end

    def close(op)
      object_id = op.use!('object')
      # The Ruby driver unified spec runner does not currently implement
      # find cursors as created by createFindCursor. This will be done
      # as part of CSOT implementation. When this is done, the line(s) below
      # should be changed to retrieve such cursor instances and close them.
      object = entities.get(:csot_cursor, object_id)
      object.close
    end
  end
end
