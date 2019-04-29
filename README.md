# Backend

**Contributors**

- [Samir Gadkari](https://github.com/samirgadkari)
- [Albert Wong](http://github.com/albert-h-wong)
- [Michael Beck](http://github.com/brit228)

[Link to trello board](https://trello.com/b/VX0UcKdA/labs-12-crime-statistics)

## API

| Endpoint | METHOD | Description | Authorization |
|---|---|---|:---:|
| [/](#health-check) | GET | Health check of backend. | &#9744; |
| [/cities](#cities-get) | GET | Get all cities in database. | &#9744; |
| [/city/{cityid}/shapes](#city-shapes-get) | GET | Get all blocks for cityid. | &#9744; |
| [/city/{cityid}/data](#city-data-get) | GET | Get all data for cityid. | &#9744; |

### Health Check

#### Return Model

##### 200
```js
{
    "error": "none",
    "data": "Health check good."
}
```

## City

### Cities [GET]

#### URL Parameters

| Parameter | Definition | Example |
|---|---|---|
| `cityid` | id of city | `/city/1/data` |

#### Query Parameters

| Parameter | Definition | Example |
|---|---|---|
| `q` | search term | `q=chicago` |

#### Return Model

##### 200

```js
{
    "error": "none",
    "cities": [
        {
            "id": {cityid},
            "string": {cityname}
        },
        ...
    ]
}
```

### City Shapes [GET]

#### Return Model

##### 200

```js
{
    "error": "none",
    "blocks": [
        {
            "id": {blockid},
            "shape": [
                [
                    [{latitude}, {longitude}],
                ...],
            ...]
        },
        ...
    ],
    "citycoords": [{longitude}, {latitude}]
}
```

### City Data [GET]

#### URL Parameters

| Parameter | Definition | Example |
|---|---|---|
| `cityid` | id of city | `/city/7/data` |

#### Query Parameters

| Parameter | Definition | Example |
|---|---|---|
| `s_d` | start date | `s_d=2%2F2012` |
| `e_d` | end date | `e_d=12%2F2019` |
| `s_t` | start time | `s_t=10` |
| `e_t` | end time | `e_t=20` |
| `blockid` | block id | `blockid=72` |
| `dotw` | days of the week | `dotw=0,3,4,5` |
| `crimetypes` | primary type and descriptions of crime | `crimetypes=CRIMINAL%20DAMAGE%20%7C%20TO%20VEHICLE,THEFT%20%7C%20FROM%20BUILDING` |
| `crimeprim` | primary types of crime | `crimeprim=ARSON,ASSAULT,BATTERY` |

#### Return Model

##### 200

```js
{
    "error": "none",
    "main": {
        "blockid": {blockid},
        "values_time": [
            {{key}: {value}},
        ...],
        "values_month": [
            {{key}: {value}}
        ,...],
        "values_dow": [
            {{key}: {value}}
        ,...],
        "values_type": [
            {{key}: {value}},
        ...]
    },
    "other": [
        {
            "blockid": {blockid},
            "values": [{value},...]
        },
        ...
    ],
    "timeline": [
        {
            "month": {month},
            "year": {year}
        },
        ...
    ]
}
```
