autoload :AwsUtils, 'support/aws_utils'

class EcsSetup
  def run
    opts = {
      region: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_REGION'),
      access_key_id: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ACCESS_KEY_ID'),
      secret_access_key: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_SECRET_ACCESS_KEY'),
    }

    inspector = AwsUtils::Inspector.new(**opts)

    cluster = inspector.ecs_client.describe_clusters(
      clusters: [ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ECS_CLUSTER_ARN')],
    ).clusters.first

    orchestrator = AwsUtils::Orchestrator.new(**opts)

    service_name = "mdb-ruby_test_#{SecureRandom.uuid}"
    puts "Using service name: #{service_name}"

    service = orchestrator.provision_auth_ecs_task(
      cluster_name: cluster.cluster_name,
      service_name: service_name,
      security_group_id: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ECS_SECURITY_GROUP'),
      subnet_ids: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ECS_SUBNETS').split(','),
      task_definition_ref: ENV.fetch('MONGO_RUBY_DRIVER_AWS_AUTH_ECS_TASK_DEFINITION_ARN'),
    )

    puts "Waiting for #{service_name} to become ready"
    orchestrator.wait_for_ecs_ready(
      cluster_name: cluster.cluster_name,
      service_name: service_name,
    )
    puts "... OK"

    status = inspector.ecs_status(
      cluster_name: cluster.cluster_name,
      service_name: service.service_name,
      get_public_ip: false, get_logs: false,
    )

    # Wait for the task to provision itself. In Evergreen I assume the image
    # already comes with SSH configured therefore this step is probably not
    # needed, but when we test using the driver tooling there is a reasonably
    # lengthy post-boot provisioning process that we need to wait for to
    # complete.
    begin
      Timeout.timeout(180) do
        begin
          Timeout.timeout(5) do
            # The StrictHostKeyChecking=no option is important here.
            # Note also that once this connection succeeds, this option
            # need not be passed again when connecting to the same IP.
            puts "Try to connect to #{status[:private_ip]}"
            puts `ssh -o StrictHostKeyChecking=no root@#{status[:private_ip]} id`
          end
        rescue Timeout::Error
          retry
        end
      end
    rescue Timeout::Error
      raise 'The task did not provision itself in 3 minutes'
    end

    File.open('.env.private.ecs', 'w') do |f|
      status.each do |k, v|
        f << "#{k.upcase}=#{v}\n"
      end
    end
  end
end
