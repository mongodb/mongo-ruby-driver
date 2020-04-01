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

    def set_instance_profile(instance_id)
      clear_instance_profile(instance_id)

      ec2_client.associate_iam_instance_profile(
        iam_instance_profile: {name: AWS_AUTH_INSTANCE_PROFILE_NAME},
        instance_id: instance_id,
      )
    end

    def clear_instance_profile(instance_id)
      assoc = detect_object(ec2_client.describe_iam_instance_profile_associations,
        :iam_instance_profile_associations, :instance_id, instance_id)
      if assoc
        ec2_client.disassociate_iam_instance_profile(
          association_id: assoc.association_id,
        )
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

    def provision_auth_ecs_task(public_key_path)
      public_key = File.read(public_key_path)
      security_group_id = ssh_vpc_security_group_id!
      subnet_id = subnet_id!

      # https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ECS_AWSCLI_Fargate.html
      resp = ecs_client.describe_clusters(
        clusters: [AWS_AUTH_ECS_CLUSTER_NAME],
      )
      cluster = detect_object(resp, :clusters, :cluster_name, AWS_AUTH_ECS_CLUSTER_NAME)
      if cluster.nil?
        raise 'No cluster found, please run `aws setup-resources`'
      end

      execution_role = detect_object(iam_client.list_roles, :roles, :role_name, AWS_AUTH_ECS_ROLE_NAME)
      if execution_role.nil?
        raise 'Execution role not configured'
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
        sleep 3600 &&
        tail -f /var/log/auth.log
        #mkdir /run/sshd && /usr/sbin/sshd -d
CMD
  #entry_point=['bash','-c','echo hai']
      task_definition = ecs_client.register_task_definition(
        family: AWS_AUTH_ECS_TASK_FAMILY,
        container_definitions: [{
          name: 'ssh',
          essential: true,
          entry_point: entry_point,
          image: 'debian:10',
          #image: 'httpd:2.4',
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
        requires_compatibilities: ['FARGATE'],
        network_mode: 'awsvpc',
        cpu: '256',
        memory: '2048',
        execution_role_arn: execution_role.arn,
      ).task_definition

      service = ecs_client.describe_services(
        cluster: AWS_AUTH_ECS_CLUSTER_NAME,
        services: [AWS_AUTH_ECS_SERVICE_NAME],
      ).services.first

      if service
        service = ecs_client.update_service(
          cluster: AWS_AUTH_ECS_CLUSTER_NAME,
          service: AWS_AUTH_ECS_SERVICE_NAME,
          task_definition: AWS_AUTH_ECS_TASK_FAMILY,
        ).service
      else
        service = ecs_client.create_service(
          desired_count: 1,
          service_name: AWS_AUTH_ECS_SERVICE_NAME,
          task_definition: AWS_AUTH_ECS_TASK_FAMILY,
          cluster: AWS_AUTH_ECS_CLUSTER_NAME,
          launch_type: 'FARGATE',
          network_configuration: {
            awsvpc_configuration: {
              subnets: [subnet_id],
              security_groups: [security_group_id],
              assign_public_ip: 'ENABLED',
            },
          },
        ).service
      end
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
