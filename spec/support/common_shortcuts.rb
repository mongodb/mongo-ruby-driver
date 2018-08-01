module CommonShortcuts
  # Declares a topology double, which is configured to accept summary
  # calls as those are used in SDAM event creation
  def declare_topology_double
    let(:topology) do
      double('topology').tap do |topology|
        allow(topology).to receive(:summary)
      end
    end
  end
end

