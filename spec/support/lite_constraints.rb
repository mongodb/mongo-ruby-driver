module LiteConstraints
  # Constrain tests that use TimeoutInterrupt to MRI (and Unix)
  def only_mri
    before do
      unless SpecConfig.instance.mri?
        skip "MRI required, we have #{SpecConfig.instance.platform}"
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
end
