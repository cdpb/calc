### Calculate distance from wordpress 'WP Googlemaps'

### Setup

```
CREATE TABLE wordpress.wp_cdpb_calc (
  id INT(6) PRIMARY KEY,
  description VARCHAR(1000) NOT NULL,
  distance INT(30) NOT NULL,
  method VARCHAR(20) NOT NULL,
  skipcalculate varchar(20) DEFAULT NULL,
  PRIMARY KEY (id)
);

#### Environment
- GOOGLE_APIKEY
- MYSQL_HOST
- MYSQL_USER
- MYSQL_PASSWORD

docker-compose.yml
```
calc:
 build: calc
 image: calc:1.0.2
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

Example use in functions.php
```
// summary of distance from maps plugin
function get_cdpb_calc() {
        global $wpdb;
        $a = $wpdb->get_results("SELECT SUM(distance) as count FROM wp_cdpb_calc where skipcalculate is not true");
        $b = $a[0]->count;
        $b = $b / 1000;
        $b = ceil($b);
        return $b;
}
add_shortcode( 'wp-cdpb-calc', 'get_cdpb_calc' );
```

To skip calculaten
```
UPDATE wp_cdpb_calc SET skipcalculate = true WHERE id = xx;
```
