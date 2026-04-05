# Distributed Data Systems Project Template

Basic project structure with Python's Flask and Redis. 
**You are free to use any web framework in any language and any database you like for this project.**

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

##### Docker Compose size configurations

Use these exact commands from the repository root:

Default (base file):

```bash
docker compose up --build
```

Small (1 replica for `order-service`, `stock-service`, `payment-service`):

```bash
docker compose -f docker-compose.yml -f docker-compose.small.yml up -d --build
```

Medium (3 replicas each):

```bash
docker compose -f docker-compose.yml -f docker-compose.medium.yml up -d
```

Large (5 replicas each):

```bash
docker compose -f docker-compose.yml -f docker-compose.large.yml up -d
```

Demo Medium (50 total containers):

```bash
docker compose -f docker-compose.yml -f docker-compose.demo_medium.yml up -d --build
```

Demo Large (90 total containers):

```bash
docker compose -f docker-compose.yml -f docker-compose.demo_large.yml up -d --build
```

Unix/Linux/macOS helper script:

```bash
chmod +x run-demo-scaling.sh
```

```bash
./run-demo-scaling.sh small
./run-demo-scaling.sh medium
./run-demo-scaling.sh large
```

The script includes `--build` in all three Docker Compose commands.

Demo allocation details:
- Fixed infrastructure (6 containers): `gateway=1`, `order-db=1`, `stock-db=1`, `payment-db=1`, `zookeeper=1`, `kafka=1`
- Demo Medium scaled services (44 containers, near-even split): `order-service=15`, `stock-service=15`, `payment-service=14`
- Demo Large scaled services (84 containers, even split): `order-service=28`, `stock-service=28`, `payment-service=28`

XLarge (10 replicas each):

```bash
docker compose -f docker-compose.yml -f docker-compose.xlarge.yml up -d
```

XXLarge (20 replicas each):

```bash
docker compose -f docker-compose.yml -f docker-compose.xxlarge.yml up -d
```

Optional manual scaling example:

```bash
docker compose up -d --scale order-service=5 --scale stock-service=5 --scale payment-service=5
```

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.
