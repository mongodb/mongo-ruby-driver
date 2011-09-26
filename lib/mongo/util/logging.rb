module Mongo
  module Logging

    # Log a message with the given level.
    def log(level, msg)
      return unless @logger
      case level
        when :debug then
          @logger.debug "MONGODB [DEBUG] #{msg}"
        when :warn then
          @logger.warn "MONGODB [WARNING] #{msg}"
        when :error then
          @logger.error "MONGODB [ERROR] #{msg}"
        when :fatal then
          @logger.fatal "MONGODB [FATAL] #{msg}"
        else
          @logger.info "MONGODB [INFO] #{msg}"
      end
    end

    # Execute the block and log the operation described by name and payload.
    def instrument(name, payload = {}, &blk)
      before = Time.now
      res = yield
      after = Time.now
      payload[:duration] = 1000.0 * (after - before) if payload
      log_operation(name, payload)
      res
    end

    protected

    def log_operation(name, payload)
      @logger ||= nil
      return unless @logger
      msg = "#{payload[:database]}['#{payload[:collection]}'].#{name}("
      msg += payload.values_at(:selector, :document, :documents, :fields ).compact.map(&:inspect).join(', ') + ")"
      msg += ".skip(#{payload[:skip]})"  if payload[:skip]
      msg += ".limit(#{payload[:limit]})"  if payload[:limit]
      msg += ".sort(#{payload[:order]})"  if payload[:order]
      @logger.debug "MONGODB (%.1fms) #{msg}" % payload[:duration]
    end

  end
end
