# Flask-node

> Similar to python Flask web-framework



## Install

You can use `npm` or `yarn` install the Flask-node

```
npm install flask-node --save
// or
yarn add flask-node --save
```



## Usage

```typescript
/** Typescript app.ts */
import { Flask, Router, Request, Response } from 'flask-node';

const app = new Flask(__dirname);
const router = new Router();

function indexHandle(req: Request, res: Response) {
    res.end('Hello world');
}

router.add('/', indexHandle);

app.run({ port: 5051 });
```

or

```javascript
/** Javascript app.js */
const { Flask, Router } = require('flask-node');
const app = new Flask(__dirname);
const router = new Router();

function indexHandle(req, res) {
    res.end('Hello world');
}
router.add('/', indexHandle);

app.run({ port: 5051 });
```

```shell
// run app
node app.js // ==> Server is run on http://localhost:5051

// use curl test app.js
curl http://localhost:5051 // ==> Hello world
```



## Features

* Support dynamic routing
* Support HTML template engine (use [Swig](https://github.com/paularmstrong/swig]))
* Natice support HTTP body parse (use [formidable](https://github.com/felixge/node-formidable))



## Documentation

* [Tutorial](https://github.com/lleohao/flask-node/tree/master/example)
* [API](https://lleohao.github.io/flask-node/)



## Test

run `npm test`



## Contribution

1. fork this repo
2. run `npm install` install dependencies
3. write your code and run `git cz` commit your code
4. run `npm test` and ensure that all tests pass
5. pull request , tks



## License

MIT © [Lleohao](https://lleohao.github.io)
