FROM maven:3.8.4-openjdk-11
WORKDIR /suffix-tree-index
COPY . /suffix-tree-index
CMD mvn test -DindexType=$INDEX_TYPE -DindexSize=$INDEX_SIZE