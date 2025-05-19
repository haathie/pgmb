CLUSTER_NAME=pgmb-benchmark-cluster
ROLE_NAME=PGMB_Benchmark_CSI_DriverRole

eksctl utils associate-iam-oidc-provider \
	--cluster=$CLUSTER_NAME --approve
echo "associated IAM OIDC provider with cluster $CLUSTER_NAME"

eksctl create iamserviceaccount \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster $CLUSTER_NAME \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --role-only \
  --role-name $ROLE_NAME
echo "created IAM service account ebs-csi-controller-sa with role $ROLE_NAME"

eksctl create addon --name aws-ebs-csi-driver --cluster $CLUSTER_NAME --service-account-role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/$ROLE_NAME --force
echo "created EBS CSI driver addon for cluster $CLUSTER_NAME with service account role $ROLE_NAME"