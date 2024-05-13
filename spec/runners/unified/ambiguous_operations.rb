# frozen_string_literal: true

module Unified
  module AmbiguousOperations

    def find(op)
      entities.get(:collection, op['object'])
      crud_find(op)
    rescue Unified::Error::EntityMissing => e
      entities.get(:bucket, op['object'])
      gridfs_find(op)
    end
  end
end
