# frozen_string_literal: true
# rubocop:todo all

module AwsUtils
  class Inspector < Base

    def list_key_pairs
      ec2_client.describe_key_pairs.key_pairs.each do |key_pair|
        puts key_pair.key_name
      end
    end

    def assume_role_arn
      assume_role = detect_object(iam_client.list_roles, :roles, :role_name, AWS_AUTH_ASSUME_ROLE_NAME)
      if assume_role.nil?
        raise 'No user found, please run `aws setup-resources`'
      end
      assume_role.arn
    end

    def ecs_status(cluster_name: AWS_AUTH_ECS_CLUSTER_NAME,
      service_name: AWS_AUTH_ECS_SERVICE_NAME,
      get_public_ip: true, get_logs: true
    )
      service = ecs_client.describe_services(
        cluster: cluster_name,
        services: [service_name],
      ).services.first
      if service.nil?
        raise "No service #{service_name} in cluster #{cluster_name} - provision first"
      end

      # When Ruby driver tooling is used, task definition generation is
      # going up on each service launch, and service name is the fixed.
      # When testing in Evergreen, generation is fixed because we do not
      # change the task definition, but service name is different for
      # each test run.
      if service.task_definition =~ /:(\d+)$/
        generation = $1
        puts "Current task definition generation: #{generation} for service: #{service_name}"
      else
        raise 'Could not determine task definition generation'
      end

      colors = {
        'running' => :green,
        'pending' => :yellow,
        'stopped' => :red,
      }
      # Pending status in the API includes tasks in provisioning status as
      # show in the AWS console.
      #
      # The API returns the tasks unordered, in particular the latest task
      # may be in the middle of the list following relatively ancient tasks.
      # Collect all tasks in a single list and order them by generation.
      # We expect to have a single task per generation.
      tasks = []
      %w(running pending stopped).each do |status|
        resp = ecs_client.list_tasks(
          cluster: cluster_name,
          service_name: service_name,
          desired_status: status,
        )
        task_arns = resp.map(&:task_arns).flatten
        if task_arns.empty?
          next
        end
        ecs_client.describe_tasks(
          cluster: cluster_name,
          tasks: task_arns,
        ).each do |tbatch|
          unless tbatch.failures.empty?
            # The task list endpoint does not raise an exception if it can't
            # find the tasks, but reports "failures".
            puts "Failures for #{task_arns.join(', ')}:"
            tbatch.failures.each do |failure|
              puts "#{failure.arn}: #{failure.reason}"
              next
            end
          end
          tbatch.tasks.each do |task|
            tasks << task
          end
        end
      end

      tasks.each do |task|
        class << task
          def generation
            @generation ||= if task_definition_arn =~ /:(\d+)$/
              $1.to_i
            else
              raise 'Could not determine generation'
            end
          end

          def task_uuid
            @uuid ||= task_arn.split('/').last
          end
        end
      end

      tasks = tasks.sort_by do |task|
        -task.generation
      end.first(3)

      running_task = nil
      running_private_ip = nil
      running_public_ip = nil

      if tasks.empty?
        puts 'No tasks in the cluster'
      end
      tasks.each do |task|
        status = task.last_status.downcase

        status_ext = case status
        when 'stopped'
          ": #{task.stopped_reason}"
        else
          ''
        end
        decorated_status = Paint[status.upcase, colors[status]]
        puts "Task for generation #{task.generation}: #{decorated_status}#{status_ext} (uuid: #{task.task_uuid})"
        if status == 'running'
          puts "Task ARN: #{task.task_arn}"
          running_task ||= task
        end
        task.containers.each do |container|
          if container.reason
            puts container.reason
          end
        end

        if status == 'running'
          attachment = detect_object([task], :attachments, :type, 'ElasticNetworkInterface')
          ip = detect_object([attachment], :details, :name, 'privateIPv4Address')
          if ip
            private_ip = ip.value
            running_private_ip ||= private_ip
          end
          msg = "Private IP: #{private_ip}"
          if get_public_ip
            niid = detect_object([attachment], :details, :name, 'networkInterfaceId')
            network_interface = ec2_client.describe_network_interfaces(
              network_interface_ids: [niid.value],
            ).network_interfaces.first
            public_ip =  network_interface&.association&.public_ip
            running_public_ip ||= public_ip
            msg += ", public IP: #{public_ip}"
          end
          puts msg
        end
        puts
      end

      puts
      task_ids = []
      max_event_count = 5
      event_count = 0
      service = ecs_client.describe_services(
        cluster: cluster_name,
        services: [service_name],
      ).services.first
      if service.nil?
        puts 'Service is missing'
      else
        if service.events.empty?
          puts 'No events for service'
        else
          puts "Events for #{service.service_arn}:"
          service.events.each do |event|
            event_count += 1
            break if event_count > max_event_count
            if event.message =~ /\(task (\w+)\)/
              task_ids << $1
            end
            puts "#{event.created_at.strftime('%Y-%m-%d %H:%M:%S %z')} #{event.message}"
          end
        end
      end

      if get_logs && running_task
        puts
        log_stream_name = "task/ssh/#{running_task.task_uuid}"
        log_stream = logs_client.describe_log_streams(
          log_group_name: AWS_AUTH_ECS_LOG_GROUP,
          log_stream_name_prefix: log_stream_name,
        ).log_streams.first
        if log_stream
          log_events = logs_client.get_log_events(
            log_group_name: AWS_AUTH_ECS_LOG_GROUP,
            log_stream_name: log_stream_name,
            end_time: Time.now.to_i * 1000,
            limit: 100,
          ).events
          if log_events.any?
            puts "Task logs for task #{running_task.task_uuid}:"
            log_events.each do |event|
              puts "[#{Time.at(event.timestamp/1000r).strftime('%Y-%m-%d %H:%M:%S %z')}] #{event.message}"
            end
          else
            puts "No CloudWatch events in the log stream for task #{running_task.task_uuid}"
          end
        else
          puts "No CloudWatch log stream for task #{running_task.task_uuid}"
        end
      end

      if running_public_ip
        puts
        puts "ip=#{running_public_ip}"
        puts "ssh -o StrictHostKeyChecking=false root@#{running_public_ip}"
      end

      {
        private_ip: running_private_ip,
      }
    end

    private

    def ucfirst(str)
      str[0].upcase + str[1...str.length]
    end
  end
end
