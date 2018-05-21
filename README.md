### Calculate distance from wordpress 'WP Googlemaps'

### Setup

```
CREATE TABLE `wp_cdpb_calc` (
  `id` int(6) NOT NULL,
  `description` varchar(1000) NOT NULL,
  `distance` int(30) NOT NULL,
  `ident` varchar(6) NOT NULL,
  `method` varchar(20) NOT NULL,
  `dfrom` varchar(100) NOT NULL,
  `dto` varchar(100) NOT NULL,
  `skip` varchar(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
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
        $a = $wpdb->get_results("SELECT ROUND(SUM(distance)/1000) as count FROM wp_cdpb_calc where skip is not true");
        $b = $a[0]->count;
        return $b;
}
add_shortcode( 'wp-cdpb-calc', 'get_cdpb_calc' );
```

To skip calculaten
```
UPDATE wp_cdpb_calc SET skip = true WHERE id = xx;
```
