# dl

> sudo npm install

import data to mongo:

this service supports **updating the data source**  option with accepting or rejecting a record accordingly

> cd import
>
> node csv_to_mongo.js data_source/airport.csv

start service:

> npm start

API's:

### get reviews per airport
```
GET localhost:3000/api/:airport/reviews

Optional param or_min

```
 example:
```
GET localhost:3000/api/zurich-airport/reviews?or_min=5
```

### get stats per airport

```
GET localhost:3000/api/:airport/stats/
```
 example

```
GET localhost:3000/api/zurich-airport/stats/
```

### get all stats

```
GET localhost:3000/api/all/stats/
```