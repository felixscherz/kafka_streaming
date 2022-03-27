## Steps

1. setup kafka environment
2. setup python environment that publishes example data
3. setup pyspark environment to stream process the events
4. setup a database to store the results for future analysis

### 1. setup kafka environment

get kafka compose file from bitnami:
`curl -sSL https://raw.githubusercontent.com/bitnami/bitnami-docker-kafka/master/docker-compose.yml > docker-compose.yml`
