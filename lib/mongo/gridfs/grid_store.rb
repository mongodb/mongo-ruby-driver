require 'mongo/types/dbref'
require 'mongo/types/objectid'
require 'mongo/util/ordered_hash'
require 'mongo/gridfs/chunk'

module XGen
  module Mongo
    module GridFS

      # GridStore is an IO-like object that provides input and output for
      # streams of data to Mongo. See Mongo's documentation about GridFS for
      # storage implementation details.
      #
      # Example code:
      #
      #   GridStore.open(database, 'filename', 'w') { |f|
      #     f.puts "Hello, world!"
      #   }
      #   GridStore.open(database, 'filename, 'r') { |f|
      #     puts f.read         # => Hello, world!\n
      #   }
      #   GridStore.open(database, 'filename', 'w+") { |f|
      #     f.puts "But wait, there's more!"
      #   }
      #   GridStore.open(database, 'filename, 'r') { |f|
      #     puts f.read         # => Hello, world!\nBut wait, there's more!\n
      #   }
      class GridStore

        include Enumerable

        attr_accessor :filename

        # Array of strings; may be +nil+
        attr_accessor :aliases

        # Default is 'text/plain'
        attr_accessor :content_type

        attr_reader :object_id, :upload_date

        attr_reader :chunk_size

        attr_accessor :lineno

        class << self

          def exist?(db, name)
            db.collection('_files').find({'filename' => name}).next_object.nil?
          end

          def open(db, name, mode)
            gs = self.new(db, name, mode)
            result = nil
            begin
              result = yield gs if block_given?
            ensure
              gs.close
            end
            result
          end

          def read(db, name, length=nil, offset=nil)
            GridStore.open(db, name, 'r') { |gs|
              gs.seek(offset) if offset
              gs.read(length)
            }
          end

          def readlines(db, name, separator=$/)
            GridStore.open(db, name, 'r') { |gs|
              gs.readlines(separator)
            }
          end

          def unlink(db, *names)
            names.each { |name|
              gs = GridStore.new(db, name)
              gs.send(:delete_chunks)
              db.collection('_files').remove('_id' => gs.object_id)
            }
          end
          alias_method :delete, :unlink

        end

        #---
        # ================================================================
        #+++

        # Mode may only be 'r', 'w', or 'w+'.
        def initialize(db, name, mode='r')
          @db, @filename, @mode = db, name, mode

          doc = @db.collection('_files').find({'filename' => @filename}).next_object
          if doc
            @object_id = doc['_id']
            @content_type = doc['contentType']
            @chunk_size = doc['chunkSize']
            @upload_date = doc['uploadDate']
            @aliases = doc['aliases']
            @length = doc['length']
            fc_id = doc['next']
            if fc_id
              coll = @db.collection('_chunks')
              row = coll.find({'_id' => fc_id.object_id}).next_object
              @first_chunk = row ? Chunk.new(coll, row) : nil
            else
              @first_chunk = nil
            end
          else
            @upload_date = Time.new
            @chunk_size = Chunk::DEFAULT_CHUNK_SIZE
            @content_type = 'text/plain'
            @length = 0
          end

          case mode
          when 'r'
            @curr_chunk = @first_chunk
          when 'w'
            delete_chunks
            @first_chunk = @curr_chunk = nil
          when 'w+'
            @curr_chunk = find_last_chunk
            @curr_chunk.pos = @curr_chunk.data.length if @curr_chunk
          end

          @lineno = 0
          @pushback_byte = nil
        end

        # Change chunk size. Can only change if the file is opened for write
        # and the first chunk's size is zero.
        def chunk_size=(size)
          unless @mode[0] == ?w && @first_chunk == nil
            raise "error: can only change chunk size if open for write and no data written."
          end
          @chunk_size = size
        end

        #---
        # ================ reading ================
        #+++

        def getc
          if @pushback_byte
            byte = @pushback_byte
            @pushback_byte = nil
            byte
          elsif eof?
            nil
          else
            if @curr_chunk.eof?
              @curr_chunk = @curr_chunk.next
            end
            @curr_chunk.getc
          end
        end

        def gets(separator=$/)
          str = ''
          byte = getc
          return nil if byte == nil # EOF
          while byte != nil
            s = byte.chr
            str << s
            break if s == separator
            byte = getc
          end
          @lineno += 1
          str
        end

        def read(len=nil, buf=nil)
          buf ||= ''
          byte = getc
          while byte != nil && (len == nil || len > 0)
            buf << byte.chr
            len -= 1 if len
            byte = getc if (len == nil || len > 0)
          end
          buf
        end

        def readchar
          byte = getc
          raise EOFError.new if byte == nil
          byte
        end

        def readline(separator=$/)
          line = gets
          raise EOFError.new if line == nil
          line
        end

        def readlines(separator=$/)
          read.split(separator).collect { |line| "#{line}#{separator}" }
        end

        def each
          line = gets
          while line
            yield line
            line = gets
          end
        end
        alias_method :each_line, :each

        def each_byte
          byte = getc
          while byte
            yield byte
            byte = getc
          end
        end

        def ungetc(byte)
          @pushback_byte = byte
        end

        #---
        # ================ writing ================
        #+++

        def putc(byte)
          chunks = @db.collection('_chunks')
          if @curr_chunk == nil
            @first_chunk = @curr_chunk = Chunk.new(chunks, 'cn' => 1)
          elsif @curr_chunk.pos == @chunk_size
            prev_chunk = @curr_chunk
            @curr_chunk = Chunk.new(chunks, 'cn' => prev_chunk.chunk_number + 1)
            prev_chunk.next = @curr_chunk
            prev_chunk.save
          end
          @curr_chunk.putc(byte)
        end

        def print(*objs)
          objs = [$_] if objs == nil || objs.empty?
          objs.each { |obj|
            str = obj.to_s
            str.each_byte { |byte| putc(byte) }
          }
          nil
        end

        def puts(*objs)
          if objs == nil || objs.empty?
            putc(10)
          else
            print(*objs.collect{ |obj|
                    str = obj.to_s
                    str << "\n" unless str =~ /\n$/
                    str
                  })
          end
          nil
        end

        def <<(obj)
          write(obj.to_s)
        end

        # Writes +string+ as bytes and returns the number of bytes written.
        def write(string)
          raise "#@filename not opened for write" unless @mode[0] == ?w
          count = 0
          string.each_byte { |byte|
            putc byte
            count += 1
          }
          count
        end

        # A no-op.
        def flush
        end

        #---
        # ================ status ================
        #+++

        def eof
          raise IOError.new("stream not open for reading") unless @mode[0] == ?r
          @curr_chunk == nil || (@curr_chunk.eof? && !@curr_chunk.has_next?)
        end
        alias_method :eof?, :eof

        #---
        # ================ positioning ================
        #+++

        def rewind
          if @curr_chunk != @first_chunk
            @curr_chunk.save unless @curr_chunk == nil || @curr_chunk.empty?
            @curr_chunk == @first_chunk
          end
          @curr_chunk.pos = 0
          @lineno = 0
          # TODO if writing, delete all other chunks on first write
        end

        def seek(pos, whence=IO::SEEK_SET)
#           target_pos = case whence
#                        when IO::SEEK_CUR
#                          tell + pos
#                        when IO::SEEK_END
                         
#           @curr_chunk.save if @curr_chunk
#           target_chunk_num = ((pos / @chunk_size) + 1).to_i
#           target_chunk_pos = pos % @chunk_size
#           if @curr_chunk == nil || @curr_chunk.chunk_number != target_chunk_num
#           end
#           @curr_chunk.pos = target_chunk_pos
#           0
          # TODO
          raise "not yet implemented"
        end

        def tell
          return 0 unless @curr_chunk
          @chunk_size * (@curr_chunk.chunk_number - 1) + @curr_chunk.pos
        end

        #---
        # ================ closing ================
        #+++

        def close
          if @mode[0] == ?w
            if @curr_chunk
              @curr_chunk.truncate
              @curr_chunk.save
            end
            files = @db.collection('_files')
            if @object_id
              files.remove('_id' => @object_id)
            else
              @object_id = XGen::Mongo::Driver::ObjectID.new
            end
            files.insert(to_mongo_object)
          end
          @db = nil
        end
          
        def closed?
          @db == nil
        end

        #---
        # ================ protected ================
        #+++

        protected

        def to_mongo_object
          h = OrderedHash.new
          h['_id'] = @object_id
          h['filename'] = @filename
          h['contentType'] = @content_type
          h['length'] = @curr_chunk ? (@curr_chunk.chunk_number - 1) * @chunk_size + @curr_chunk.pos : 0
          h['chunkSize'] = @chunk_size
          h['uploadDate'] = @upload_date
          h['aliases'] = @aliases
          h['next'] = XGen::Mongo::Driver::DBRef.new(nil, nil, @db, '_chunks', @first_chunk.object_id) if @first_chunk
          h
        end

        def find_last_chunk
          chunk = @curr_chunk || @first_chunk
          while chunk.has_next?
            chunk = chunk.next
          end
          chunk
        end

        def save_chunk(chunk)
          chunks = @db.collection('_chunks')
        end

        def delete_chunks
          chunk = @first_chunk
          coll = @db.collection('_chunks')
          while chunk
            next_chunk = chunk.next
            coll.remove({'_id' => chunk.object_id})
            chunk = next_chunk
          end
          @first_chunk = @curr_chunk = nil
        end

      end
    end
  end
end
