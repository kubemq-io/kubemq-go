# Go
KubeMQ is an enterprise-grade message queue and broker for containers, designed for any workload and architecture running in Kubernetes.
This library is Go implementation of KubeMQ client connection.

## Get Started

### Deploy KubeMQ Cluster
#### Option 1:
Get KubeMQ Cluster with default settings - [Quick Start](https://kubemq.io/quick-start/)

#### Option 2:
Build and Deploy KubeMQ Cluster with advanced configurations - [Build & Deploy](https://build.kubemq.io/)

### Port-Forward KubeMQ Grpc Interface

Use kubectl to port-forward kubemq grpc interface 
```
kubectl port-forward svc/kubemq-cluster-grpc 50000:50000 -n kubemq
```


## Examples - Cookbook Recipes
Please visit our cookbook [repository](https://github.com/kubemq-io/go-sdk-cookbook)


## Support
if you encounter any issues, please open an issue here,
In addition, you can reach us for support by:
- [**Email**](mailto://support@kubemq.io)
- [**Slack**](https://kubmq.slack.com)
