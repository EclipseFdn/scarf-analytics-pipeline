## How to deploy staging instance for a given image?

```bash
./helm-deploy.sh staging <docker_image_tag>
```

Where `<docker_image_tag>` can be de4f2c

## Preparing for EF JIRO specific environment
Since EF [JIRO] runs with specific user, `namespace-rbac.yaml` grants the
`ci-bot` ServiceAccount `admin` rights scoped to the `openvsx-scarf-analytics`
namespace, so Jenkins can deploy. An EF infra admin (or whoever has
namespace-admin rights on `openvsx-scarf-analytics`) must apply it:

```bash
kubectl apply -f namespace-rbac.yaml
```

## Dependencies

* bash 4
* [Helm](https://https://helm.sh/)
