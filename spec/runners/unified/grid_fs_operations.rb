# frozen_string_literal: true
# encoding: utf-8

module Unified

  module GridFsOperations

    def delete(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        bucket.delete(args.use!('id'))
      end
    end

    def download(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        stream = bucket.open_download_stream(args.use!('id'))
        stream.read
      end
    end

    def upload(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if chunk_size = args.use('chunkSizeBytes')
          opts[:chunk_size] = chunk_size
        end
        contents = transform_contents(args.use!('source'))
        file_id = nil
        bucket.open_upload_stream(args.use!('filename'), **opts) do |stream|
          stream.write(contents)
          file_id = stream.file_id
        end
        file_id
      end
    end

    private

    def transform_contents(contents)
      if Hash === contents
        if contents.length != 1
          raise NotImplementedError, "Wanted hash with one element"
        end
        if contents.keys.first != '$$hexBytes'
          raise NotImplementedError, "$$hexBytes is the only key supported"
        end

        decode_hex_bytes(contents.values.first)
      else
        contents
      end
    end

  end
end
