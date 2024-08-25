# suffix-tree-index-experiments

This project is designed to test the performance of string search operations using a suffix tree index and various databases, including Elasticsearch, PostgreSQL, MySQL, and ClickHouse. The experiment environment is implemented using the JUnit Jupiter engine.

## Project Structure

    * Indexes Implementations: Located in the `src/main/java/com/dima_z/indexes` directory.
	* Datasource: The dataset, consisting of 2 million rows, is stored in the `./data` folder.
	* Results: The results of the performance tests can be found in the `./results` folder.

## Project Requirements

	* Java: Version 11
	* Apache Maven: Version 3.9

To launch the project please check src/main/resources/config.properties.template to configure the credentials for the mentioned database envirovement. (the credentials should be in config.properties file)

## Possible commands to launch the project:

```bash
mvn test -DindexType=$indexType -DindexSize=$indexSize
```

* $indexType options
	- SUFFIX_TREE
	- ELASTIC_TREE
	- MYSQL_TREE
	- POSTGRES_TREE
	- CLICKHOUSE_TREE

### Example:

```bash
mvn test -DindexType=SUFFIX_TREE -DindexSize=50000
```

## Docker command template:

```bash
docker run --env=INDEX_TYPE=SUFFIX_TREE --env=INDEX_SIZE=50000 --volume=./data:/suffix-tree-index-experiments/data --volume=./results:/suffix-tree-index-experiments/results --network=experiment-network suffix-tree-index-experiments
```
