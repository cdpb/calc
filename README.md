### Calculate distance from wordpress 'WP Googlemaps'

### Setup

```
CREATE TABLE wordpress.wp_cdpb_calc (
  id INT(6) PRIMARY KEY,
  description VARCHAR(250) NOT NULL,
  distance INT(30) NOT NULL,
  method VARCHAR(20) NOT NULL
);
```

#### Environment
- GOOGLE_APIKEY
- MYSQL_HOST
- MYSQL_USER
- MYSQL_PASSWORD

docker-compose.yml
```
calc:
 build: calc
 image: calc:1.0.0
 environment:
   MYSQL_DATABASE: _DB_
   MYSQL_USER: _USER_
   MYSQL_PASSWORD: _PASSWORD_
   MYSQL_HOST: 172.16.0.X
   GOOGLE_APIKEY: _APIKEY_
 networks:
   wordpress:
     ipv4_address: 172.16.0.X
```

crontab
```
@daily root /usr/bin/docker-compose --file docker-compose.yml up calc > /dev/null 2>&1

```
