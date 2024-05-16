# frozen_string_literal: true
# rubocop:todo all

module Unified

  module GridFsOperations

    def gridfs_find(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        filter = args.use!('filter')

        opts = extract_options(args, 'allowDiskUse',
          'skip', 'hint','timeoutMS',
          'noCursorTimeout', 'sort', 'limit')

        bucket.find(filter,opts).to_a
      end
    end

    def delete(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if timeout_ms = args.use('timeoutMS')
          opts[:timeout_ms] = timeout_ms
        end
        bucket.delete(args.use!('id'), opts)
      end
    end

    def download(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if timeout_ms = args.use('timeoutMS')
          opts[:timeout_ms] = timeout_ms
        end
        stream = bucket.open_download_stream(args.use!('id'), opts)
        stream.read
      end
    end

    def download_by_name(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if revision = args.use('revision')
          opts[:revision] = revision
        end
        stream = bucket.open_download_stream_by_name(args.use!('filename'), opts)
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
        if metadata = args.use('metadata')
          opts[:metadata] = metadata
        end
        if content_type = args.use('contentType')
          opts[:content_type] = content_type
        end
        if disable_md5 = args.use('disableMD5')
          opts[:disable_md5] = disable_md5
        end
        if timeout_ms = args.use('timeoutMS')
          opts[:timeout_ms] = timeout_ms
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

    def drop(op)
      bucket = entities.get(:bucket, op.use!('object'))
      use_arguments(op) do |args|
        opts = {}
        if timeout_ms = args.use('timeoutMS')
          opts[:timeout_ms] = timeout_ms
        end
        bucket.drop(opts)
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
