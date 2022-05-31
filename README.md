# Telemetry data Extractor

[![Build Status](https://travis-ci.com/keboola/ex-telemetry-data.svg?branch=master)](https://travis-ci.com/keboola/ex-telemetry-data)

> Exports telemetry data for specified project 

# Usage

```json
{
  "parameters": {},
  "image_parameters": {
    "db": {
      "host": "XXX",
      "port": 3306,
      "user": "XXX",
      "database": "XXX",
      "#password": "xxx"
    }
  }
}
```

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/ex-telemetry-data
cd ex-telemetry-data
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```

## License

MIT licensed, see [LICENSE](./LICENSE) file.
