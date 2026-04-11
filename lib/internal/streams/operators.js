'use strict';

const {
  ArrayPrototypePush,
  ArrayFrom,
  Boolean,
  MathFloor,
  NumberMAX_SAFE_INTEGER,
  Number,
  NumberIsNaN,
  Promise,
  PromiseAllSettled,
  PromisePrototypeThen,
  PromiseReject,
  PromiseResolve,
  PromiseWithResolvers,
  Symbol,
  SymbolIterator,
} = primordials;

const { AbortController, AbortSignal } = require('internal/abort_controller');

const {
  AbortError,
  aggregateTwoErrors,
  codes: {
    ERR_MISSING_ARGS,
    ERR_OUT_OF_RANGE,
  },
} = require('internal/errors');
const {
  validateAbortSignal,
  validateInteger,
  validateObject,
  validateFunction,
  validateBoolean,
} = require('internal/validators');
const { kWeakHandler, kResistStopPropagation } = require('internal/event_target');
const { eos, finished } = require('internal/streams/end-of-stream');
const destroyImpl = require('internal/streams/destroy');

const kEmpty = Symbol('kEmpty');
const kEof = Symbol('kEof');

function map(fn, options) {
  validateFunction(fn, 'fn');
  if (options != null) {
    validateObject(options, 'options');
  }
  if (options?.signal != null) {
    validateAbortSignal(options.signal, 'options.signal');
  }

  let concurrency = 1;
  if (options?.concurrency != null) {
    concurrency = MathFloor(options.concurrency);
  }

  let highWaterMark = concurrency - 1;
  if (options?.highWaterMark != null) {
    highWaterMark = MathFloor(options.highWaterMark);
  }

  validateInteger(concurrency, 'options.concurrency', 1);
  validateInteger(highWaterMark, 'options.highWaterMark', 0);

  highWaterMark += concurrency;

  return function map() {
    const signal = AbortSignal.any([options?.signal].filter(Boolean));
    const source = this;
    const queue = [];
    const signalOpt = { signal };

    let continueReadingFromQueue;
    let resumeReadingFromSource;
    let continueReadingFromSource;
    let stopReadingFromSource = false;
    let currentConcurrency = 0;

    function onCatch() {
      stopReadingFromSource = true;
      afterItemProcessed();
      if (continueReadingFromQueue) {
        continueReadingFromQueue();
        continueReadingFromQueue = null;
      }
    }

    function afterItemProcessed() {
      currentConcurrency -= 1;
      maybeResumeReadingFromSource();
    }

    function maybeResumeReadingFromSource() {
      if (
        resumeReadingFromSource &&
        !stopReadingFromSource &&
        currentConcurrency < concurrency &&
        queue.length < highWaterMark
      ) {
        resumeReadingFromSource();
        resumeReadingFromSource = null;
      }
    }

    async function readFromSource() {
      try {

        function moreDataToReadFromSource() {
          if (continueReadingFromSource) {
            continueReadingFromSource();
            continueReadingFromSource = null;
          }
        }

        source.on('readable', moreDataToReadFromSource);

        let error;
        const cleanup = eos(source, { writable: false }, (err) => {
          error = err ? aggregateTwoErrors(error, err) : null;
          moreDataToReadFromSource();
        });

        try {
          while (true) {
            if (stopReadingFromSource) {
              return;
            }

            if (signal.aborted) {
              throw new AbortError();
            }

            if (queue.length >= highWaterMark || currentConcurrency >= concurrency) {
              const pr = PromiseWithResolvers();
              resumeReadingFromSource = pr.resolve;
              await pr.promise;
            }

            let chunk = source.destroyed ? null : source.read();
            if (chunk !== null) {
              currentConcurrency += 1;
              chunk = fn(chunk, signalOpt);

              if (chunk === kEmpty) {
                afterItemProcessed();
                continue;
              }
              if (typeof chunk.then === 'function') {
                PromisePrototypeThen(PromiseResolve(chunk), afterItemProcessed, onCatch);
                queue.push(chunk);
              } else {
                queue.push({ chunk, afterItemProcessed });
              }
              if (continueReadingFromQueue) {
                continueReadingFromQueue();
                continueReadingFromQueue = null;
              }
              if (chunk === kEof) {
                // Wait until queue is drained before exiting.
                while (queue.length > 0) {
                  const pr = PromiseWithResolvers();
                  resumeReadingFromSource = pr.resolve;
                  await pr.promise;
                }
                return;
              }

              if (!stopReadingFromSource && (queue.length >= highWaterMark || currentConcurrency >= concurrency)) {
                const pr = PromiseWithResolvers();
                resumeReadingFromSource = pr.resolve;
                await pr.promise;
              }
            } else if (error) {
              throw error;
            } else if (error === null) {
              queue.push(kEof);
              return;
            } else {
              const pr = PromiseWithResolvers();
              continueReadingFromSource = pr.resolve;
              await pr.promise;
            }
          }
        } catch (err) {
          error = aggregateTwoErrors(error, err);
          throw error;
        } finally {
          if (
            (error || options?.destroyOnReturn !== false) &&
            (error === undefined || source._readableState.autoDestroy)
          ) {
            destroyImpl.destroyer(source, null);
          } else {
            source.off('readable', moreDataToReadFromSource);
            cleanup();
          }
        }
      } catch (err) {
        const val = PromiseReject(err);
        PromisePrototypeThen(val, afterItemProcessed, onCatch);
        queue.push(val);
      } finally {
        stopReadingFromSource = true;
        if (continueReadingFromQueue) {
          continueReadingFromQueue();
          continueReadingFromQueue = null;
        }
      }
    }

    process.nextTick(readFromSource);

    const readable = new source.constructor({
      objectMode: true,
      highWaterMark,
    });
    let currentlyReadingFromQueue = false;
    readable._read = async function readFromQueue() {
      try {
        if (!currentlyReadingFromQueue) {
          currentlyReadingFromQueue = true;

          if (!stopReadingFromSource && queue.length === 0) {
            const pr = PromiseWithResolvers();
            continueReadingFromQueue = pr.resolve;
            await pr.promise;
          }
          while (queue.length > 0) {
            if (signal.aborted) {
              currentlyReadingFromQueue = false;
              throw new AbortError();
            }

            let item = queue[0];
            let syncAip = null;
            if (typeof item.then === 'function') {
              item = await item;
              queue.shift();
            } else if (typeof item.afterItemProcessed === 'function') {
              syncAip = item.afterItemProcessed;
              item = item.chunk;
              // Call syncAip BEFORE shifting so queue.length stays >= 1,
              // matching async item behavior (afterItemProcessed fires via
              // PromiseThen while item is still peeked). This prevents
              // maybeResumeReadingFromSource from immediately waking readFromSource.
              syncAip();
              queue.shift();
            } else {
              queue.shift();
            }

            if (item === kEof) {
              stopReadingFromSource = true;
              currentlyReadingFromQueue = false;
              this.push(null);
              if (resumeReadingFromSource) {
                resumeReadingFromSource();
                resumeReadingFromSource = null;
              }
              if (continueReadingFromQueue) {
                continueReadingFromQueue();
                continueReadingFromQueue = null;
              }
              return;
            }
            if (item !== kEmpty) {
              if (syncAip) {
                // Sync item: yield via await so that stream.read()'s preemptive
                // _read call (triggered when state.length-n < hwm) sees kReading
                // still set and does not re-enter _read before push clears it.
                // Schedule maybeResume as a nextTick (not a microtask) so that
                // the async-iterator cleanup microtask (done=true) from a
                // consumer break runs before pump is woken, preventing an extra
                // fn call on infinite streams.
                await PromiseResolve();
                if (!this.push(item)) {
                  currentlyReadingFromQueue = false;
                  process.nextTick(maybeResumeReadingFromSource);
                  return;
                }
                process.nextTick(maybeResumeReadingFromSource);
              } else if (!this.push(item)) {
                currentlyReadingFromQueue = false;
                process.nextTick(maybeResumeReadingFromSource);
                return;
              } else {
                process.nextTick(maybeResumeReadingFromSource);
              }
            } else {
              if (syncAip) syncAip();
              maybeResumeReadingFromSource();
              if (!stopReadingFromSource && queue.length === 0) {
                const pr = PromiseWithResolvers();
                continueReadingFromQueue = pr.resolve;
                await pr.promise;
              }
              continue;
            }
          }
          currentlyReadingFromQueue = false;
        }
      } catch (err) {
        destroyImpl.destroyer(this, err);
      }
    }
    readable._destroy = function(err, cb) {
      stopReadingFromSource = true;
      if (resumeReadingFromSource) {
        resumeReadingFromSource();
        resumeReadingFromSource = null;
      }
      if (continueReadingFromQueue) {
        continueReadingFromQueue();
        continueReadingFromQueue = null;
      }
      cb(err);
    };
    return readable;
  }.call(this);
}

function nowOrLater(fn, fn2, args) {
  const valueOrPromise = fn(...args);
  if (valueOrPromise && typeof valueOrPromise.then === 'function') {
      return PromisePrototypeThen(valueOrPromise, fn2);
    }
    return fn2(valueOrPromise);
}

async function some(fn, options = undefined) {
  validateFunction(fn, 'fn');
  const someFn = (...args) => {
    return nowOrLater(fn, Boolean, args);
  };
  return (await find.call(this, someFn, options)) !== undefined;
}

async function every(fn, options = undefined) {
  validateFunction(fn, 'fn');
  const everyFn = (...args) => {
    return nowOrLater(fn, value => !value, args);
  };
  // https://en.wikipedia.org/wiki/De_Morgan%27s_laws
  return !(await some.call(this, everyFn, options));
}

function find(fn, options) {
  validateFunction(fn, 'fn');

  if (options != null) {
    validateObject(options, 'options');
  }
  const signal = options?.signal;
  if (signal != null) {
    validateAbortSignal(signal, 'options.signal');
  }

  let concurrency = 1;
  if (options?.concurrency != null) {
    concurrency = MathFloor(options.concurrency);
  }
  validateInteger(concurrency, 'options.concurrency', 1);

  let destroyOnReturn = true;
  if (options?.destroyOnReturn != null) {
    destroyOnReturn = options?.destroyOnReturn;
    validateBoolean(destroyOnReturn, 'options.destroyOnReturn');
  }

  let result = undefined;
  let foundIndex = undefined;
  let currentConcurrency = 0;
  let sequence = 0;
  const pr = PromiseWithResolvers();
  this.on('error', pr.reject);

  const onData = async function (chunk) {
    if (foundIndex !== undefined && foundIndex < sequence) {
      this.off('data', onData);
      return;
    }
    let i = sequence;
    sequence++;
    currentConcurrency++;
    if (currentConcurrency >= concurrency && !this.isPaused()) {
      this.pause();
    }
    try {
      if (signal?.aborted) {
        throw new AbortError(undefined, { cause: signal.reason });
      }
      let v = fn(chunk, options);
      if (v && typeof v.then === 'function') {
        v = await v;
      }

      if (v && (foundIndex === undefined || foundIndex > i)) {
        foundIndex = i;
        result = chunk;
      }
    } catch (err) {
      this.off('data', onData);
      destroyImpl.destroyer(this, err);
    } finally {
      currentConcurrency--;
      if (this.isPaused() && currentConcurrency < concurrency && foundIndex === undefined) {
        this.resume();
      }
      if (currentConcurrency === 0 && foundIndex !== undefined) {
        this.off('data', onData);
        pr.resolve(result);
      }
    }
  };
  this.on('data', onData);
  this.on('end', function onEnd() {
    this.off('data', onData);
    foundIndex = NumberMAX_SAFE_INTEGER;
    if (currentConcurrency === 0) {
      pr.resolve(result);
    }
  });
  if (destroyOnReturn) {
    return PromisePrototypeThen(pr.promise, value => {
      if (!this.errored) {
        destroyImpl.destroyer(this, null);
      }
      return value;
    });
  }

  return pr.promise;
}

async function forEach(fn, options) {
  validateFunction(fn, 'fn');
  const forEachFn = (...args) => {
    return nowOrLater(fn, () => false, args);
  };
  await find.call(this, forEachFn, options);
}

function filter(fn, options) {
  validateFunction(fn, 'fn');
  const filterFn = (value, options) => {
    return nowOrLater(fn, predicate => predicate ? value : kEmpty, [value, options]);
  }
  return map.call(this, filterFn, options);
}

// Specific to provide better error to reduce since the argument is only
// missing if the stream has no items in it - but the code is still appropriate
class ReduceAwareErrMissingArgs extends ERR_MISSING_ARGS {
  constructor() {
    super('reduce');
    this.message = 'Reduce of an empty stream requires an initial value';
  }
}

async function reduce(reducer, initialValue, options) {
  validateFunction(reducer, 'reducer');
  if (options != null) {
    validateObject(options, 'options');
  }
  if (options?.signal != null) {
    validateAbortSignal(options.signal, 'options.signal');
  }

  let hasInitialValue = arguments.length > 1;
  if (options?.signal?.aborted) {
    const err = new AbortError(undefined, { cause: options.signal.reason });
    this.once('error', () => {}); // The error is already propagated
    await finished(this.destroy(err));
    throw err;
  }
  const ac = new AbortController();
  const signal = ac.signal;
  if (options?.signal) {
    const opts = { once: true, [kWeakHandler]: this, [kResistStopPropagation]: true };
    options.signal.addEventListener('abort', () => ac.abort(), opts);
  }
  let gotAnyItemFromStream = false;
  try {
    for await (const value of this) {
      gotAnyItemFromStream = true;
      if (options?.signal?.aborted) {
        throw new AbortError();
      }
      if (!hasInitialValue) {
        initialValue = value;
        hasInitialValue = true;
      } else {
        initialValue = await reducer(initialValue, value, { signal });
      }
    }
    if (!gotAnyItemFromStream && !hasInitialValue) {
      throw new ReduceAwareErrMissingArgs();
    }
  } finally {
    ac.abort();
  }
  return initialValue;
}

async function toArray(options) {
  const result = [];
  const toArrayFn = (chunk) => ArrayPrototypePush(result, chunk);
  await forEach.call(this, toArrayFn, options);
  return result;
}

function flatMap(fn, options) {
  const values = map.call(this, fn, options);
  // TODO lucho
  return async function* flatMap() {
    for await (const val of values) {
      yield* val;
    }
  }.call(this);
}

function toIntegerOrInfinity(number) {
  // We coerce here to align with the spec
  // https://github.com/tc39/proposal-iterator-helpers/issues/169
  number = Number(number);
  if (NumberIsNaN(number)) {
    return 0;
  }
  if (number < 0) {
    throw new ERR_OUT_OF_RANGE('number', '>= 0', number);
  }
  return number;
}

function drop(number, options = undefined) {
  if (options != null) {
    validateObject(options, 'options');
  }
  if (options?.signal != null) {
    validateAbortSignal(options.signal, 'options.signal');
  }

  number = toIntegerOrInfinity(number);
  return filter.call(this, () => {
    if (number > 0) {
      number--;
      return false;
    }
    return true;
  }, options);
}

function take(number, options = undefined) {
  if (options != null) {
    validateObject(options, 'options');
  }
  if (options?.signal != null) {
    validateAbortSignal(options.signal, 'options.signal');
  }

  number = toIntegerOrInfinity(number);
  return async function* take() {
    if (options?.signal?.aborted) {
      throw new AbortError();
    }
    for await (const val of this) {
      if (options?.signal?.aborted) {
        throw new AbortError();
      }
      if (number-- > 0) {
        yield val;
      }

      // Don't get another item from iterator in case we reached the end
      if (number <= 0) {
        return;
      }
    }
  }.call(this);
}

module.exports.streamReturningOperators = {
  drop,
  filter,
  flatMap,
  map,
  take,
};

module.exports.promiseReturningOperators = {
  every,
  forEach,
  reduce,
  toArray,
  some,
  find,
};
