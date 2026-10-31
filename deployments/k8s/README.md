# Kubernetes deployment

These manifests deploy the backend's five Go services, PostgreSQL, Redis, a
baseline database migration Job, Prometheus, and Grafana. The Kind overlay adds
MinIO for local media storage. The production overlay expects external
S3-compatible storage. All application and infrastructure Services are
cluster-internal; only the Core API, WebSocket Gateway, and (for Kind) MinIO
media endpoint are routed by Ingress.

Kustomize overlays are provider-neutral and do not install a Kubernetes
distribution, ingress controller, storage provisioner, or certificate
controller. Install an ingress controller supported by your cluster before
using the Ingress resources. StatefulSets request PVCs without selecting a
storage class, so the cluster's default provisioner is used.

## Local Kind

Prerequisites: Docker, Kind, kubectl with Kustomize support, and an ingress
controller installed in the Kind cluster. Configure the Kind cluster and
controller so local traffic can reach the controller on ports 80 and (if using
TLS) 443. Add `messenger.local` to the local hosts file, pointing to that
address.

Build and load application and migration images:

```sh
make k8s-load-kind
```

Create the namespace and provide the Firebase service account. The notification
service uses Application Default Credentials and requires this file even when
the other services are used without push notifications:

```sh
kubectl create namespace messenger --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic firebase-service-account -n messenger \
  --from-file=service-account.json=/absolute/path/to/firebase-service-account.json
```

Apply the Kind overlay and wait for PostgreSQL, MinIO, and the schema migration:

```sh
kubectl apply -k deployments/k8s/overlays/kind
kubectl wait -n messenger --for=condition=Ready pod -l app=postgres --timeout=180s
kubectl wait -n messenger --for=condition=Ready pod -l app=minio --timeout=180s
kubectl wait -n messenger --for=condition=Complete job/db-migrate --timeout=180s
kubectl wait -n messenger --for=condition=Complete job/minio-bucket-init --timeout=180s
```

The checked-in Kind credentials are development-only examples. Replace them
before using the cluster for anything beyond local development. They are not
used by the production overlay.

The local Ingress routes `/v1` to Core API, `/ws` to WS Gateway, and
`/chat-media` to MinIO. This makes the S3-compatible presigned media URLs
returned by Core API reachable from a browser on the host. The MinIO console
and its service port remain private.

Kind uses HTTP by default. To use a local self-signed certificate, create a
certificate whose SAN includes `messenger.local`, then create the referenced
Secret and apply the TLS overlay:

```sh
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
  -keyout /tmp/messenger-local.key -out /tmp/messenger-local.crt \
  -subj '/CN=messenger.local' \
  -addext 'subjectAltName=DNS:messenger.local'
kubectl create secret tls messenger-local-tls -n messenger \
  --cert=/tmp/messenger-local.crt --key=/tmp/messenger-local.key
kubectl apply -k deployments/k8s/overlays/kind-tls
```

Use `kubectl port-forward -n messenger service/grafana 3000:3000` to open
Grafana at <http://127.0.0.1:3000>. The local example credentials are
`admin` / `local-only-change-me`; change them before use.

## Generic or production cluster

Edit the production overlay's example image registry, public hostname, S3
endpoint, region, and bucket before applying it. The external S3 endpoint must
be reachable by clients because Core API returns presigned URLs directly to
them. Supply credentials and Firebase configuration as Secrets; for example:

```sh
kubectl create namespace messenger --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic postgres-auth -n messenger \
  --from-literal=username='messenger' \
  --from-literal=password='replace-me' \
  --from-literal=database='messenger'
kubectl create secret generic backend-secrets -n messenger \
  --from-literal=jwt-secret='replace-with-at-least-32-random-bytes' \
  --from-literal=dev-otp-code='replace-with-a-random-value' \
  --from-literal=media-access-key='replace-me' \
  --from-literal=media-secret-key='replace-me'
kubectl create secret generic grafana-auth -n messenger \
  --from-literal=username='admin' --from-literal=password='replace-me'
kubectl create secret generic firebase-service-account -n messenger \
  --from-file=service-account.json=/absolute/path/to/firebase-service-account.json
kubectl apply -k deployments/k8s/overlays/production
```

The production Ingress expects a TLS Secret named `messenger-tls`. Provision it
through the cluster's certificate workflow, or install cert-manager and use the
optional `production-cert-manager` overlay after editing its DNS name and
`issuerRef` to match the installed issuer. The overlay does not install
cert-manager or create a cloud-specific issuer. For HTTP-only staging, remove
the `ingress-tls.yaml` patch from the production overlay.

The current Core API S3 presigner requires access and secret key values. The
existing implementation does not consume AWS workload identity credentials;
using role-based signing will require extending that implementation. Keep
static keys in a Secret if required, and use the storage provider's workload
identity for other components where supported.

The current OTP implementation uses one configured code (`DEV_OTP_CODE`) for
verification. A random secret value avoids publishing the code in manifests,
but this is still development authentication and is not suitable for a public
production service until a real OTP provider is implemented.

Configure the external bucket's CORS policy for the deployed client origins and
the signed `GET`, `HEAD`, and `PUT` requests. Preserve the public endpoint host
in the ingress proxy so SigV4 host signatures remain valid.

## WebSocket ingress and shutdown

The Ingress uses standard Kubernetes routing and does not set controller
annotations. Configure the selected ingress controller's upstream read and
send timeouts to exceed expected WebSocket session duration and ensure it
supports WebSocket upgrades. For ingress-nginx, the optional
`production-nginx` overlay sets both proxy timeouts to one hour; use the
controller's equivalent settings for other implementations.

On termination, WS Gateway sends WebSocket close code 1001 (Going Away), then
closes the HTTP listener. The pod has a 45-second termination grace period and
a short `preStop` drain delay so endpoint removal can propagate before active
clients are asked to reconnect. Clients should reconnect and perform their
normal sync flow.

## Database migration and persistence

The migration image packages only
`shared/migrations/000001_init.up.sql`. Its Job waits for PostgreSQL, checks for
existing public schema objects, and refuses to run the baseline against a
non-empty schema. It does not apply seeds. To rerun a completed Job after
reviewing the database state, delete that Job and apply the overlay again; do
not use this baseline to upgrade an existing database.

PostgreSQL, Redis, and Kind MinIO use persistent volume claims. Prometheus and
Grafana also keep their data on PVCs. Redis enables AOF and the expiry events
required by presence cleanup. All PVCs omit `storageClassName` and rely on the
cluster default. Back up persistent data according to the cluster's storage
provider policy.

Prometheus scrapes each service through internal DNS. Grafana, Prometheus,
PostgreSQL, Redis, MinIO's console, and internal gRPC endpoints are not exposed
through public Ingress routes. Application logs remain available through
`kubectl logs`; centralized log collection is deferred.

## Build and render for another cluster

Build and push the five application images and `deployments/k8s/migration.Dockerfile`
to a registry reachable by the cluster, then set the `images` entries in the
production overlay. Render an overlay before applying it:

```sh
kubectl kustomize deployments/k8s/overlays/kind
kubectl kustomize deployments/k8s/overlays/production
kubectl apply -k deployments/k8s/overlays/production
```

Use `kubectl get pods,pvc,ingress -n messenger` and
`kubectl logs -n messenger job/db-migrate` to inspect startup and migration
status. A failed migration keeps database-dependent application pods in their
schema-wait init container until the issue is corrected.
