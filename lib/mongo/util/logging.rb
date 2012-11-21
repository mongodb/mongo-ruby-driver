module Mongo
  module Logging

    def write_logging_startup_message
      log(:debug, "Logging level is currently :debug which could negatively impact " +
          "client-side performance. You should set your logging level no lower than " +
          ":info in production.")
    end

    # Log a message with the given level.
    def log(level, msg)
      return unless @logger
      case level
        when :fatal then
          @logger.fatal "MONGODB [FATAL] #{msg}"
        when :error then
          @logger.error "MONGODB [ERROR] #{msg}"
        when :warn then
          @logger.warn "MONGODB [WARNING] #{msg}"
        when :info then
          @logger.info "MONGODB [INFO] #{msg}"
        when :debug then
          @logger.debug "MONGODB [DEBUG] #{msg}"
        else
          @logger.debug "MONGODB [DEBUG] #{msg}"
      end
    end

    # Execute the block and log the operation described by name and payload.
    def instrument(name, payload = {})
      start_time = Time.now
      res = Logging.instrumenter.instrument(name, payload) do
        yield
      end
      duration = Time.now - start_time
      log_operation(name, payload, duration)
      res
    end

    def self.instrumenter
      @instrumenter || Instrumenter
    end

    def self.instrumenter=(instrumenter)
      @instrumenter = instrumenter
    end

    protected

    def log_operation(name, payload, duration)
      @logger && @logger.debug do
        msg = "MONGODB "
        msg << "(%.1fms) " % (duration * 1000)
        msg << "#{payload[:database]}['#{payload[:collection]}'].#{name}("
        msg << payload.values_at(:selector, :document, :documents, :fields ).compact.map(&:inspect).join(', ') + ")"
        msg << ".skip(#{payload[:skip]})"   if payload[:skip]
        msg << ".limit(#{payload[:limit]})" if payload[:limit]
        msg << ".sort(#{payload[:order]})"  if payload[:order]
        msg
      end
    end

    module Instrumenter
      def self.instrument(name, payload = {})
        yield
      end
    end
  end
end
