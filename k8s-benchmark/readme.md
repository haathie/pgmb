# Setup Benchmarks

The code here allows for quick setup on an EKS cluster for the benchmarks.

## Provisioning

1. Have an AWS account with an active card. Make sure you have an AWS profile added to your local machine.
2. Install:
	- EKS CLI, see [here](https://eksctl.io/getting-started/)
	- Kubectl, see [here](https://kubernetes.io/docs/tasks/tools/)
3. Create the cluster: `eksctl create cluster -f k8s-benchmark/cluster.yaml`
	- Note: set `AWS_PROFILE=your_profile` if you have multiple profiles.
4. Switch to the new cluster: `aws eks update-kubeconfig --name pgmb-benchmark-cluster`
5. Run the benchmarks: `kubectl apply -f k8s-benchmark/benchmark-pgmb.yaml`. Note that the pods may crash initially, but they will restart and run the benchmarks.
6. You can check the logs with `kubectl logs -f <pod-name> -n pgmb-benchmark`. The output logs can be used to then graph the results. Terminate the pods when done.