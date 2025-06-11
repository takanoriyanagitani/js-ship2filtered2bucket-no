import { bind } from "./io.mjs";

import { FilterResult } from "./bloom.mjs";

/** @import { IO } from "./io.mjs" */

/** @typedef {number} BloomTiny16bits */

/** @typedef {number} BucketSerialSmall */

/** @typedef {Uint8Array} BloomSmall32B */

/**
 * @typedef {object} BucketBloomInfo16bits
 * @property {BucketSerialSmall} serial
 * @property {BloomTiny16bits} bloomData
 */

/** @typedef {Array<BucketBloomInfo16bits>} BucketBloomValues16bits */

/** @typedef {Map<BucketSerialSmall, BloomTiny16bits>} BucketSerialMap */

/**
 * @template P
 * @template D
 * @typedef {object} ShipId<P,D>
 * @property {P} primaryId
 * @property {D} dateInfo
 */

/**
 * @template P
 * @template D
 * @typedef {function(D): IO<Map<P, BloomSmall32B>>} ShipBloomSource<P,D>
 */

/** @typedef {number} Hash4x8bits */

/** @typedef {number} Hash4x4bits */

/**
 * @template P
 * @template D
 * @template F
 * @param {F} filterInfo
 * @param {function(F): IO<Hash4x8bits>} filter2hash
 * @param {function(BloomSmall32B, Hash4x8bits): FilterResult} hash2result
 * @param {D} date
 * @param {ShipBloomSource<P,D>} shipSource
 * @returns {IO<Array<P>>}
 */
export function getShipNumbers(
  filterInfo,
  filter2hash,
  hash2result,
  date,
  shipSource,
) {
  /** @type IO<Map<P, BloomSmall32B>> */
  const im = shipSource(date);

  /** @type IO<Hash4x8bits> */
  const ihash = filter2hash(filterInfo);

  return bind(
    im,
    (id2bloom) => {
      return async () => {
        /** @type Promise<Hash4x8bits> */
        const phash = ihash();

        /** @type Hash4x8bits */
        const hash4x8bits = await phash;

        /** @type IteratorObject<[P, BloomSmall32B]> */
        const pairs = id2bloom[Symbol.iterator]().filter((pair) => {
          const [_key, val] = pair;

          /** @type FilterResult */
          const res = hash2result(val, hash4x8bits);

          return FilterResult.MAY_EXIST === res;
        });

        /** @type IteratorObject<P> */
        const keys = pairs.map((pair) => pair[0]);

        return Promise.resolve(Array.from(keys));
      };
    },
  );
}

/**
 * @template P
 * @template D
 * @template F
 * @param {F} filterInfo
 * @param {function(F): IO<Hash4x4bits>} filter2hash
 * @param {function(BloomTiny16bits, Hash4x4bits): FilterResult} hash2result
 * @param {D} date
 * @param {P} shipId
 * @param {function(P,D): IO<BucketSerialMap>} bloomSource
 * @returns {IO<Array<BucketSerialSmall>>}
 */
export function getBucketNumbers(
  filterInfo,
  filter2hash,
  hash2result,
  date,
  shipId,
  bloomSource,
) {
  /** @type IO<BucketSerialMap> */
  const ibuckets = bloomSource(shipId, date);

  /** @type IO<Hash4x4bits> */
  const ihash = filter2hash(filterInfo);

  return bind(
    ibuckets,
    (bucketMap) => {
      return async () => {
        /** @type Promise<Hash4x4bits> */
        const phash = ihash();

        /** @type Hash4x4bits */
        const hash = await phash;

        /** @type IteratorObject<[BucketSerialSmall, BloomTiny16bits]> */
        const filtered = bucketMap[Symbol.iterator]().filter(
          (pair) => {
            const [_key, val] = pair;

            /** @type FilterResult */
            const res = hash2result(val, hash);

            return FilterResult.MAY_EXIST === res;
          },
        );

        /** @type IteratorObject<BucketSerialSmall> */
        const mapd = filtered.map((pair) => pair[0]);

        return Promise.resolve(Array.from(mapd));
      };
    },
  );
}
