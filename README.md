# Iceberg Java API Sample

Iceberg Javaの基本APIを使用した、Iceberg データ投入用のサンプルコードです。

## 手順

Icebergをローカルで実験するための、
REST Catalog, RDBMS, Object Storage(minio as local S3) については docker コンテナで提供します。

```
docker compose up -d
export AWS_REGION=us-west-2
./mvnw test
```

check insert data from minio `http://localhost:9090`  
credential is `admin/password`



