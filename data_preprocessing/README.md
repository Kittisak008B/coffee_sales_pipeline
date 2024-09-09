# Cloud SQL
```bash
gcloud sql connect your_Instance_ID --user=root --quiet
```

```sql
CREATE DATABASE demo_database01;
```

```sql
SHOW DATABASES;
```

```sql
USE demo_database01;
```

create table in database
```sql
CREATE TABLE Dummy_01 (personID INT PRIMARY KEY AUTO_INCREMENT , name VARCHAR(50) , day DATE , value FLOAT);
```

```sql
SHOW TABLES;
```

```sql
DESCRIBE Dummy_01;
```

### Check your public IP address and allow it to access the Cloud SQL instance
```bash
curl https://checkip.amazonaws.com/
```
connections->networking->add a network
