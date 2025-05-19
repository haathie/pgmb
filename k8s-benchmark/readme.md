# Setup Benchmarks

The code here allows for quick setup on an EKS cluster for the benchmarks. 

The benchmark tests are quite straightforward, we simultaneously publish and consume as many `1Kb` messages as possible from the queue (like one would in a real-world scenario). The consumer & producer are run in separate processes.

For PGMB and PGMQ, we delete the messages on "ack", rather than archive them to squeeze out the most performance. Of course, we could try it out with an unlogged table for even more performance, but that would not be fair to RabbitMQ, as we're using durable queues there.

## Provisioning

1. Have an AWS account with an active card. Make sure you have an AWS profile added to your local machine.
2. Install:
	- EKS CLI, see [here](https://eksctl.io/getting-started/)
	- Kubectl, see [here](https://kubernetes.io/docs/tasks/tools/)
3. Create the cluster: `eksctl create cluster -f k8s-benchmark/cluster.yaml`
	- Note: set `AWS_PROFILE=your_profile` if you have multiple profiles.
4. Switch to the new cluster: `aws eks update-kubeconfig --name pgmb-benchmark-cluster`
5. Give permissions to the cluster to attach to EBS volumes: `sh k8s-benchmark/provision-ebs.sh`
5. Run the benchmarks: `kubectl apply -f k8s-benchmark/benchmark-pgmb.yaml`. Note that the pods may crash initially, but they will restart and run the benchmarks.
6. You can check the logs with `kubectl logs -f <pod-name> -n pgmb-benchmark`. The output logs can be used to then graph the results. Terminate the pods when done.

Note: this will cost you some money, so make sure to delete the cluster when you're done. Also if you want to run more rigorous benchmarks, you can increase the size of the node in the `cluster.yaml` file, and increase the provisioned size of the postgres, consumer, publisher pods.