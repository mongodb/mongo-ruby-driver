module Mongo
  module Logging

    DEBUG_LEVEL = defined?(Logger) ? Logger::DEBUG : 0

    def write_logging_startup_message
      if @logger && (@logger.level == DEBUG_LEVEL)
        log(:debug, "Logging level is currently :debug which could negatively impact " +
          "client-side performance. You should set your logging level no lower than " +
          ":info in production.")
      end
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
    def instrument(name, payload = {}, &blk)
      start_time = Time.now
      res = yield
      if @logger && (@logger.level == DEBUG_LEVEL)
        log_operation(name, payload, start_time)
      end
      res
    end

    protected

    def log_operation(name, payload, start_time)
      msg = "MONGODB "
      msg << "(#{((Time.now - start_time) * 1000).to_i}ms) "
      msg << "#{payload[:database]}['#{payload[:collection]}'].#{name}("
      msg << payload.values_at(:selector, :document, :documents, :fields ).compact.map(&:inspect).join(', ') + ")"
      msg << ".skip(#{payload[:skip]})"   if payload[:skip]
      msg << ".limit(#{payload[:limit]})" if payload[:limit]
      msg << ".sort(#{payload[:order]})"  if payload[:order]

      @logger.debug(msg)
    end

  end
end
