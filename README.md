# technical-assignment

## How To Run

### Locally

```bash
# make pipelines executable
chmod -R +x pipelines

# install dependencies
uv venv
uv pip sync requirements.txt

# run the pipeline
uv run pipelines/full
```

### Docker

1. Build the docker image:

   ```bash
   docker build -t pipeline-runner .
   ```

1. Run the pipeline:

   ```bash
   docker run --rm \
     --mount type=bind,source=$(pwd)/data-lake,target=/app/data-lake \
     pipeline-runner /bin/bash pipelines/full
   ```

## (TODO) Assumptions

## (TODO) Structure

## (TODO) Challenges
