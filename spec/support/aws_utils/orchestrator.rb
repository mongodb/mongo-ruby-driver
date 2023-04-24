# frozen_string_literal: true
# rubocop:todo all

require 'securerandom'

module AwsUtils
  class Orchestrator < Base

    def assume_role(role_arn)
      # https://docs.aws.amazon.com/sdk-for-ruby/v3/api/Aws/STS/Client.html#assume_role-instance_method
      resp = sts_client.assume_role(
        role_arn: role_arn,
        role_session_name: "#{NAMESPACE}.test",
      )
      resp.credentials
    end

    def assume_role_with_web_identity(role_arn, token_file)
      token = File.open(token_file).read
      resp = sts_client.assume_role_with_web_identity(
        role_arn: role_arn,
        role_session_name: SecureRandom.uuid,
        web_identity_token: token,
        duration_seconds: 900
      )
      resp.credentials
    end

    def set_instance_profile(instance_id,
      instance_profile_name: AWS_AUTH_INSTANCE_PROFILE_NAME,
      instance_profile_arn: nil
    )
      clear_instance_profile(instance_id)

      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
      begin
        ec2_client.associate_iam_instance_profile(
          iam_instance_profile: {
            name: instance_profile_name,
            arn: instance_profile_arn,
          },
          instance_id: instance_id,
        )
      rescue Aws::EC2::Errors::RequestLimitExceeded => e
        if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          raise
        end
        STDERR.puts("AWS request limit exceeded: #{e.class}: #{e}, will retry")
        sleep 5
        retry
      end
    end

    def clear_instance_profile(instance_id)
      assoc = detect_object(ec2_client.describe_iam_instance_profile_associations,
        :iam_instance_profile_associations, :instance_id, instance_id)

      if assoc
        deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 30
        begin
          ec2_client.disassociate_iam_instance_profile(
            association_id: assoc.association_id,
          )
        rescue Aws::EC2::Errors::RequestLimitExceeded => e
          if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
            raise
          end
          STDERR.puts("AWS request limit exceeded: #{e.class}: #{e}, will retry")
          sleep 5
          retry
        end
      end
    end

    def provision_auth_ec2_instance(key_pair_name: nil, public_key_path: nil,
      distro: 'ubuntu1604'
    )
      security_group_id = ssh_security_group_id!
      reservations = ec2_client.describe_instances(filters: [
        {name: 'tag:name', values: [AWS_AUTH_EC2_INSTANCE_NAME]},
      ]).reservations
      instance = find_running_instance(reservations)
      if instance.nil?
        ami_name = AWS_AUTH_EC2_AMI_NAMES.fetch(distro)
        image = ec2_client.describe_images(
          filters: [{name: 'name', values: [ami_name]}],
        ).images.first
        if public_key_path
          public_key = File.read(public_key_path)
          user_data = Base64.encode64(<<-CMD)
#!/bin/sh
for user in `ls /home`; do
cd /home/$user &&
mkdir -p .ssh &&
chmod 0700 .ssh &&
chown $user:$user .ssh &&
cat <<-EOT |tee -a .ssh/authorized_keys
#{public_key}
EOT
done
CMD
        end
        resp = ec2_client.run_instances(
          instance_type: 't3a.small',
          image_id: image.image_id,
          min_count: 1,
          max_count: 1,
          key_name: key_pair_name,
          user_data: user_data,
          tag_specifications: [{
            resource_type: 'instance',
            tags: [{key: 'name', value: AWS_AUTH_EC2_INSTANCE_NAME}],
          }],
          monitoring: {enabled: false},
          credit_specification: {cpu_credits: 'standard'},
          security_group_ids: [security_group_id],
          metadata_options: {
            # This is required for Docker containers on the instance to be able
            # to use the instance metadata endpoints.
            http_put_response_hop_limit: 2,
          },
        ).to_h
        instance_id = resp[:instances].first[:instance_id]
        reservations = ec2_client.describe_instances(instance_ids: [instance_id]).reservations
        instance = find_running_instance(reservations)
      end
      if instance.nil?
        raise "Instance should have been found here"
      end
      if instance.state.name == 'stopped'
        ec2_client.start_instances(instance_ids: [instance.instance_id])
      end
      10.times do
        if %w(stopped pending).include?(instance.state.name)
          puts "Waiting for instance #{instance.instance_id} to start (current state: #{instance.state.name})"
          sleep 5
        end
        reservations = ec2_client.describe_instances(instance_ids: [instance.instance_id]).reservations
        instance = find_running_instance(reservations)
      end
      puts "Found usable instance #{instance.instance_id} at #{instance.public_ip_address}"
    end

    def terminate_auth_ec2_instance
      ec2_client.describe_instances(filters: [
        {name: 'tag:name', values: [AWS_AUTH_EC2_INSTANCE_NAME]},
      ]).each do |resp|
        resp.reservations.each do |res|
          res.instances.each do |instance|
            puts "Terminating #{instance.instance_id}"
            ec2_client.terminate_instances(instance_ids: [instance.instance_id])
          end
        end
      end
    end

    def provision_auth_ecs_task(public_key_path: nil,
      cluster_name: AWS_AUTH_ECS_CLUSTER_NAME,
      service_name: AWS_AUTH_ECS_SERVICE_NAME,
      security_group_id: nil,
      subnet_ids: nil,
      task_definition_ref: AWS_AUTH_ECS_TASK_FAMILY
    )
      security_group_id ||= ssh_vpc_security_group_id!
      subnet_ids ||= [subnet_id!]

      # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_AWSCLI_Fargate.html
      resp = ecs_client.describe_clusters(
        clusters: [cluster_name],
      )
      cluster = detect_object(resp, :clusters, :cluster_name, cluster_name)
      if cluster.nil?
        raise 'No cluster found, please run `aws setup-resources`'
      end

      if public_key_path
        public_key = File.read(public_key_path)
        unless public_key =~ /\Assh-/
          raise "The file at #{public_key_path} does not look like a public key"
        end
        entry_point = ['bash', '-c', <<-CMD]
          apt-get update &&
          apt-get install -y openssh-server &&
          cd /root &&
          mkdir -p .ssh &&
          chmod 0700 .ssh &&
          cat >.ssh/authorized_keys  <<-EOT &&
#{public_key}
EOT
          service ssh start &&
          sleep 10000000
          #mkdir /run/sshd && /usr/sbin/sshd -d
CMD
      else
        entry_point = nil
      end
      launch_type = if options[:ec2]
        'EC2'
      else
        'FARGATE'
      end
      # When testing in Evergreen, we are given the task definition ARN
      # and we always launch the tasks with that ARN.
      # When testing locally, we repace task definition every time we launch
      # the service.
      if task_definition_ref !~ /^arn:/
        execution_role = detect_object(iam_client.list_roles, :roles, :role_name, AWS_AUTH_ECS_EXECUTION_ROLE_NAME)
        if execution_role.nil?
          raise 'Execution role not configured'
        end
        task_role = detect_object(iam_client.list_roles, :roles, :role_name, AWS_AUTH_ECS_TASK_ROLE_NAME)
        if task_role.nil?
          raise 'Task role not configured'
        end

        task_definition = ecs_client.register_task_definition(
          family: AWS_AUTH_ECS_TASK_FAMILY,
          container_definitions: [{
            name: 'ssh',
            essential: true,
            entry_point: entry_point,
            image: 'debian:9',
            port_mappings: [{
              container_port: 22,
              protocol: 'tcp',
            }],
            log_configuration: {
              log_driver: 'awslogs',
              options: {
                'awslogs-group' => AWS_AUTH_ECS_LOG_GROUP,
                'awslogs-region' => region,
                'awslogs-stream-prefix' => AWS_AUTH_ECS_LOG_STREAM_PREFIX,
              },
            },
          }],
          requires_compatibilities: [launch_type],
          network_mode: 'awsvpc',
          cpu: '512',
          memory: '2048',
          # This is the ECS task role used for AWS auth testing
          task_role_arn: task_role.arn,
          # The execution role is required to support awslogs (logging to
          # CloudWatch).
          execution_role_arn: execution_role.arn,
        ).task_definition
        task_definition_ref = AWS_AUTH_ECS_TASK_FAMILY
      end

      service = ecs_client.describe_services(
        cluster: cluster_name,
        services: [service_name],
      ).services.first

      if service && service.status.downcase == 'draining'
        puts "Waiting for #{service_name} to drain"
        ecs_client.wait_until(
          :services_inactive, {
            cluster: cluster.cluster_name,
            services: [service_name],
          },
          delay: 5,
          max_attempts: 36,
        )
        puts "... done."
        service = nil
      end
      if service && service.status.downcase == 'inactive'
        service = nil
      end
      if service
        puts "Updating service with status #{service.status}"
        service = ecs_client.update_service(
          cluster: cluster_name,
          service: service_name,
          task_definition: task_definition_ref,
        ).service
      else
        puts "Creating a new service"
        vpc_config = {}
        unless options[:ec2]
          vpc_config[:assign_public_ip] = 'ENABLED'
        end
        service = ecs_client.create_service(
          desired_count: 1,
          service_name: service_name,
          task_definition: task_definition_ref,
          cluster: cluster_name,
          launch_type: launch_type,
          network_configuration: {
            awsvpc_configuration: vpc_config.merge(
              subnets: subnet_ids,
              security_groups: [security_group_id],
            ),
          },
        ).service
      end
    end

    def wait_for_ecs_ready(
      cluster_name: AWS_AUTH_ECS_CLUSTER_NAME,
      service_name: AWS_AUTH_ECS_SERVICE_NAME,
      timeout: 20
    )
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout

      # The AWS SDK waiter seems to immediately fail sometimes right after
      # the service is created, so wait for the service to become active
      # manually and then use the waiter to wait for the service to become
      # stable.
      #
      # The failure may be due to the fact that apparently, it is possible for
      # describe_services to not return an existing service for some time.
      # Therefore, allow the lack of service to be a transient error.
      loop do
        service = ecs_client.describe_services(
          cluster: cluster_name,
          services: [service_name],
        ).services.first

        if service.nil?
          puts "Service #{service_name} in cluster #{cluster_name} does not exist (yet?)"
          status = 'MISSING'
        elsif service.status.downcase == 'active'
          break
        else
          status = service.status
        end

        if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
          raise "Service #{service_name} in cluster #{cluster_name} did not become ready in #{timeout} seconds (current status: #{status})"
        end

        puts "Wating for service #{service_name} in cluster #{cluster_name} to become ready (#{'%2.1f' % (deadline - now)} seconds remaining, current status: #{status})"
        sleep 5
      end

      puts "Wating for service #{service_name} in cluster #{cluster_name} to become stable"
      ecs_client.wait_until(
        :services_stable, {
          cluster: cluster_name,
          services: [service_name],
        },
        delay: 5,
        max_attempts: 36,
      )
    end

    def terminate_auth_ecs_task
      ecs_client.describe_services(
        cluster: AWS_AUTH_ECS_CLUSTER_NAME,
        services: [AWS_AUTH_ECS_SERVICE_NAME],
      ).each do |resp|
        resp.services.each do |service|
          puts "Terminating #{service.service_name}"
          begin
            ecs_client.update_service(
              cluster: AWS_AUTH_ECS_CLUSTER_NAME,
              service: service.service_name,
              desired_count: 0,
            )
          rescue Aws::ECS::Errors::ServiceNotActiveException
            # No action needed
          end
          ecs_client.delete_service(
            cluster: AWS_AUTH_ECS_CLUSTER_NAME,
            service: service.service_name,
          )
        end
      end
    end

    private

    def find_running_instance(reservations)
      instance = nil
      reservations.each do |reservation|
        instance = reservation.instances.detect do |instance|
          %w(pending running stopped).include?(instance.state.name)
        end
        break if instance
      end
      instance
    end
  end
end
