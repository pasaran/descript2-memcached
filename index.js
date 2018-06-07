'use strict';

const contimer = require('contimer');
const crypto = require('crypto');
const de = require('descript2');
const Memcached = require('memcached');
const no = require('nommon');

const POOL = {};

class DescriptMemcached {

    constructor(options, context) {
        if (!context) {
            throw '[descript2-memcached] descript context is required!';
        }

        this._options = Object.assign({
            defaultKeyTTL: 60 * 60 * 24, // key ttl in seconds
            generation: 1, // increment generation to invalidate all key across breaking changes releases
            readTimeout: 100, // read timeout in milliseconds,
            memcachedOptions: {}, // @see https://github.com/3rd-Eden/memcached#options
        }, options);

        this._context = context;

        // We need de.Context for logging, de.Context creates for every request.
        // So we have a problem to establish memcached connection each time => use connection pool
        const optionsKey = JSON.stringify(options);
        if (!POOL[optionsKey]) {
            POOL[optionsKey] = new Memcached(options.servers, options.memcachedOptions);
            this._log({
                type: DescriptMemcached.EVENT.MEMCACHED_INITIALIZED,
                options: options
            });
        }
        this._client = POOL[optionsKey];
    }

    get(key) {
        const normalizedKey = this.normalizeKey(key);
        const promise = no.promise();

        const networkTimerStop = contimer.start({}, 'descript2-memcached.get.network');
        const totalTimerStop = contimer.start({}, 'descript2-memcached.get.total');

        let isTimeout = false;

        const timer = setTimeout(() => {
            isTimeout = true;

            const networkTimer = networkTimerStop();
            const totalTimer = totalTimerStop();

            this._log({
                type: DescriptMemcached.EVENT.MEMCACHED_READ_TIMEOUT,
                key,
                normalizedKey,
                timers: {
                    network: networkTimer,
                    total: totalTimer,
                },
            });

            promise.resolve(de.error({
                id: DescriptMemcached.EVENT.MEMCACHED_READ_TIMEOUT,
            }));
        }, this._options.readTimeout);

        this._client.get(normalizedKey, (error, data) => {
            if (isTimeout) {
                return;
            }

            const networkTimer = networkTimerStop();
            clearTimeout(timer);

            if (error) {
                const totalTimer = totalTimerStop();
                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_READ_ERROR,
                    error,
                    key,
                    normalizedKey,
                    timers: {
                        network: networkTimer,
                        total: totalTimer,
                    },
                });

                promise.resolve(de.error({
                    id: DescriptMemcached.EVENT.MEMCACHED_READ_ERROR,
                }));
            } else if (!data) {
                const totalTimer = totalTimerStop();
                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_READ_KEY_NOT_FOUND,
                    key,
                    normalizedKey,
                    timers: {
                        network: networkTimer,
                        total: totalTimer,
                    },
                });

                promise.resolve(de.error({
                    id: DescriptMemcached.EVENT.MEMCACHED_READ_KEY_NOT_FOUND,
                }));
            } else {
                let parsedValue;
                try {
                    parsedValue = JSON.parse(data);
                } catch (error) {
                    const totalTimer = totalTimerStop();
                    this._log({
                        type: DescriptMemcached.EVENT.MEMCACHED_JSON_PARSING_FAILED,
                        data,
                        error,
                        key,
                        normalizedKey,
                        timers: {
                            network: networkTimer,
                            total: totalTimer,
                        },
                    });

                    promise.resolve(de.error({
                        id: DescriptMemcached.EVENT.MEMCACHED_JSON_PARSING_FAILED,
                    }));
                    return;
                }

                const totalTimer = totalTimerStop();
                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_READ_DONE,
                    data,
                    key,
                    normalizedKey,
                    timers: {
                        network: networkTimer,
                        total: totalTimer,
                    },
                });

                promise.resolve(parsedValue);
            }
        });

        return promise;
    }

    set(key, data, ttl = this._options.defaultKeyTTL) {
        if (typeof data === 'undefined') {
            return;
        }

        const totalTimerStop = contimer.start({}, 'descript2-memcached.set.total');

        const normalizedKey = this.normalizeKey(key);

        let json;
        try {
            json = JSON.stringify(data);
        } catch (error) {
            const totalTimer = totalTimerStop();
            this._log({
                type: DescriptMemcached.EVENT.MEMCACHED_JSON_STRINGIFY_FAILED,
                data,
                error,
                key,
                normalizedKey,
                timers: {
                    network: {},
                    total: totalTimer,
                },
            });
            de.error({
                id: DescriptMemcached.EVENT.MEMCACHED_JSON_STRINGIFY_FAILED,
            });
            return;
        }

        const networkTimerStop = contimer.start({}, 'descript2-memcached.set.network');
        // ttl - seconds
        this._client.set(normalizedKey, json, ttl, (error, done) => {
            const networkTimer = networkTimerStop();
            const totalTimer = totalTimerStop();
            if (error) {
                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_WRITE_ERROR,
                    error,
                    key,
                    normalizedKey,
                    timers: {
                        network: networkTimer,
                        total: totalTimer,
                    },
                });
                de.error({
                    id: DescriptMemcached.EVENT.MEMCACHED_WRITE_ERROR,
                });
            } else if (!done) {
                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_WRITE_FAILED,
                    key,
                    normalizedKey,
                    timers: {
                        network: networkTimer,
                        total: totalTimer,
                    },
                });
                de.error({
                    id: DescriptMemcached.EVENT.MEMCACHED_WRITE_FAILED,
                });
            } else {
                this._log({
                    type: DescriptMemcached.EVENT.MEMCACHED_WRITE_DONE,
                    data: json,
                    key,
                    normalizedKey,
                    timers: {
                        network: networkTimer,
                        total: totalTimer,
                    },
                });
            }
        });
    }

    /**
     * Generates normalized SHA-512 key with generation
     * @param {string} key
     * @returns {string}
     */
    normalizeKey(key) {
        const value = `g${ this._options.generation }:${ key }`;
        return crypto
            .createHash('sha512')
            .update(value, 'utf8')
            .digest('hex');
    }

    _log(event) {
        this._context.logger.log(event, this._context);
    }
}

DescriptMemcached.EVENT = {
    MEMCACHED_INITIALIZED: 'MEMCACHED_INITIALIZED',

    MEMCACHED_JSON_PARSING_FAILED: 'MEMCACHED_JSON_PARSING_FAILED',
    MEMCACHED_JSON_STRINGIFY_FAILED: 'MEMCACHED_JSON_STRINGIFY_FAILED',

    MEMCACHED_READ_DONE: 'MEMCACHED_READ_DONE',
    MEMCACHED_READ_ERROR: 'MEMCACHED_READ_ERROR',
    MEMCACHED_READ_KEY_NOT_FOUND: 'MEMCACHED_READ_KEY_NOT_FOUND',
    MEMCACHED_READ_TIMEOUT: 'MEMCACHED_READ_TIMEOUT',

    MEMCACHED_WRITE_DONE: 'MEMCACHED_WRITE_DONE',
    MEMCACHED_WRITE_ERROR: 'MEMCACHED_WRITE_ERROR',
    MEMCACHED_WRITE_FAILED: 'MEMCACHED_WRITE_FAILED',
};

module.exports = DescriptMemcached;
