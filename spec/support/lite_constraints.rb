module LiteConstraints
  # Constrain tests that use TimeoutInterrupt to MRI (and Unix)
  def only_mri
    before do
      unless SpecConfig.instance.mri?
        skip "MRI required, we have #{SpecConfig.instance.platform}"
      end
    end
  end

  # This is for marking tests that fail on jruby that should
  # in principle work (as opposed to being fundamentally incompatible
  # with jruby).
  # Often times these failures happen only in Evergreen.
  def fails_on_jruby(version=nil)
    before do
      if BSON::Environment.jruby?
        if version
          min_parts = version.split('.').map(&:to_i)
          actual_parts = JRUBY_VERSION.split('.').map(&:to_i)[0...min_parts.length]
          actual = actual_parts.join('.')
          if actual <= version
            skip "Fails on jruby through #{version}"
          end
        else
          skip "Fails on jruby"
        end
      end
    end
  end

  def require_external_connectivity
    before do
      if ENV['EXTERNAL_DISABLED']
        skip "Test requires external connectivity"
      end
    end
  end

  def require_linting
    before do
      unless Mongo::Lint.enabled?
        skip "Linting is not enabled"
      end
    end
  end

  # Some tests will fail if linting is enabled:
  # 1. Tests that pass invalid options to client, etc. which the linter
  #    rejects.
  # 2. Tests that set expectations on topologies, server descriptions, etc.
  #    (since setting expectations requires mutating said objects, and when
  #    linting is on those objects are frozen).
  def skip_if_linting
    before do
      if Mongo::Lint.enabled?
        skip "Linting is enabled"
      end
    end
  end
end
