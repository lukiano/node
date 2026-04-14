
'use strict';

const common = require('../common');
const { Readable } = require('stream');

const bench = common.createBenchmark(main, {
  n: [5e6],
});

async function main({ n }) {
  const b = {};
  const r = new Readable({ objectMode: true });

  let i = 0;

  r._read = () => r.push(i++ === n ? null : b);

  bench.start();
  await r.map(i => i + 1).reduce((acc, i) => acc + i);
  bench.end(n);
}
