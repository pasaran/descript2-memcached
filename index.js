'use strict';

const crypto = require('crypto');
const de = require('descript2');
const Memcached = require('memcached');
const no = require('nommon');

class deMemcached {

    constructor(options) {
        this._options = Object.assign({
            defaultKeyTTL: 60 * 60 * 24, // key ttl in seconds
            generation: 1, // increment generation to invalidate all key across breaking changes releases
            readTimeout: 100, // read timeout in milliseconds,
            memcachedOptions: {}, // @see https://github.com/3rd-Eden/memcached#options
        }, options);

        if (!options.servers) {
            throw '[descript2-memcached] options.servers is empty!';
        }

        this._client = new Memcached(options.servers, options.memcachedOptions);
    }

    get(key) {
        const normalizedKey = this.normalizeKey(key);
        const promise = no.promise();

        const timer = setTimeout(() => {
            promise.resolve(de.error({
                id: 'MEMCACHED_READ_TIMEOUT',
                key: key,
                normalizedKey: normalizedKey,
            }));
        }, this._options.readTimeout);

        this._client.get(normalizedKey, function(error, data) {
            clearTimeout(timer);

            if (error) {
                promise.resolve(de.error({
                    id: 'MEMCACHED_READ_ERROR',
                    error: error,
                    key: key,
                    normalizedKey: normalizedKey,
                }));
            } else if (!data) {
                promise.resolve(de.error({
                    id: 'MEMCACHED_KEY_NOT_FOUND',
                    key: key,
                    normalizedKey: normalizedKey,
                }));
            } else {
                let parsedValue;
                try {
                    parsedValue = JSON.parse(data);
                } catch (err) {
                    promise.resolve(de.error({
                        id: 'MEMCACHED_JSON_PARSING_FAILED',
                        value: data,
                        error: error,
                        key: key,
                        normalizedKey: normalizedKey,
                    }));
                    return;
                }

                promise.resolve(parsedValue);
            }
        });

        return promise;
    }

    set(key, value, ttl = this._options.defaultKeyTTL) {
        if (typeof value === 'undefined') {
            return;
        }

        const normalizedKey = this.normalizeKey(key);

        let json;
        try {
            json = JSON.stringify(value);
        } catch (error) {
            // TODO: log
            de.error({
                id: 'MEMCACHED_JSON_STRINGIFY_FAILED',
                value: value,
                error: error,
                key: key,
                normalizedKey: normalizedKey,
            });
            return;
        }

        // ttl - seconds
        this._client.set(normalizedKey, json, ttl, function(error, done) {
            if (error) {
                // TODO: log
                de.error({
                    id: 'MEMCACHED_WRITE_ERROR',
                    error: error,
                    key: key,
                    normalizedKey: normalizedKey,
                });
            } else if (!done) {
                de.error({
                    id: 'MEMCACHED_WRITE_FAILED',
                    key: key,
                    normalizedKey: normalizedKey,
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
}

module.exports = deMemcached;
