import { readFile } from "node:fs/promises";

import {
  bigint2hashSmall,
  bigint2hashTiny,
  bloom2resultSmall,
  bloom2resultTiny,
  bloomUpdateSmall,
  bloomUpdateTiny,
  FilterResult,
} from "./bloom.mjs";

import { getBucketNumbers, getShipNumbers } from "./index.mjs";

/** @import { IO } from "./io.mjs" */

/** @import { Hash4x8bits, BloomSmall32B } from "./index.mjs" */

/** @import { BucketSerialSmall, BucketSerialMap } from "./index.mjs" */
/** @import { BloomTiny16bits, Hash4x4bits } from "./index.mjs" */

/**
 * @typedef { object } FilterInfo
 * @property { BigInt } orderId
 */

/** @type FilterInfo */
const filterInfo = Object.freeze({
  orderId: 1771104931734n,
});

const date = "2025/06/11";

const shipId = "cafef00d-dead-beaf-face-864299792458";

/** @type function(): function(FilterInfo): IO<Hash4x4bits> */
const filter2hashBucketNew = () => {
  const buf = new BigInt64Array(1);
  const view = new DataView(buf.buffer);
  return (finfo) => bigint2hashTiny(finfo.orderId, view);
};

/** @type function(): function(FilterInfo): IO<Hash4x8bits> */
const filter2hashNew = () => {
  const buf = new BigInt64Array(1);
  const view = new DataView(buf.buffer);
  return (finfo) => bigint2hashSmall(finfo.orderId, view);
};

const main = async () => {
  /** @type function(FilterInfo): IO<Hash4x8bits> */
  const filter2hash = filter2hashNew();

  /** @type IO<Hash4x8bits> */
  const ihash = bigint2hashSmall(
    filterInfo.orderId,
    new DataView(new BigInt64Array(1).buffer),
  );

  /** @type IO<Hash4x8bits> */
  const ihash42 = bigint2hashSmall(
    42n,
    new DataView(new BigInt64Array(1).buffer),
  );

  /** @type Array<Hash4x8bits> */
  const hash32values = await Promise.all([
    ihash(),
    ihash42(),
  ]);

  /** @type BloomSmall32B */
  const bdata = new Uint8Array(32);

  hash32values.forEach((hval) => {
    bloomUpdateSmall(bdata, hval);
  });

  /** @type BloomSmall32B */
  const allTrue = new Uint8Array(32);
  allTrue.fill(0xff);

  /** @type Map<string, BloomSmall32B> */
  const smap0611 = new Map([
    ["cafef00d-dead-beaf-face-864299792458", bdata],
    ["dafef00d-dead-beaf-face-864299792458", allTrue],
  ]);

  /** @type Map<string, Map<string, BloomSmall32B>> */
  const dmap = new Map([
    ["2025/06/11", smap0611],
    ["2025/06/12", new Map()],
    ["2025/06/10", new Map()],
  ]);

  /** @type function(string): IO<Map<string, BloomSmall32B>> */
  const shipSource = function (date) {
    return () => {
      /** @type Map<string, BloomSmall32B>? */
      const omap = dmap.get(date) ?? null;

      if (!omap) return Promise.resolve(new Map());

      /** @type Map<string, BloomSmall32B> */
      const smap = omap;

      return Promise.resolve(smap);
    };
  };

  /** @type IO<Array<string>> */
  const isnums = getShipNumbers(
    filterInfo,
    filter2hash,
    bloom2resultSmall,
    date,
    shipSource,
  );

  /** @type Array<string> */
  const uuids = await isnums();

  for (const uuid of uuids) {
    console.info(`candidate ship: ${uuid}`);
  }

  /** @type BloomTiny16bits */
  const bword = await bigint2hashTiny(
    1771104931734n,
    new DataView(new BigInt64Array(1).buffer),
  )();

  /** @type BloomTiny16bits */
  const bwdata = bloomUpdateTiny(0, bword);

  /** @type Map<BucketSerialSmall, BloomTiny16bits> */
  const bmap0611 = new Map([
    [4093, bwdata],
    [2047, 0xffff],
    [1023, 0x0000],
  ]);

  const bmap = new Map([
    ["cafef00d-dead-beaf-face-864299792458/2025/06/11", bmap0611],
    ["dafef00d-dead-beaf-face-864299792458/2025/06/11", bmap0611],
  ]);

  /** @type function(string, string): IO<BucketSerialMap> */
  const bloomSource = function (shipId, date) {
    return () => {
      /** @type string */
      const id = `${shipId}/${date}`;

      /** @type Map<BucketSerialSmall, BloomTiny16bits>? */
      const obm = bmap.get(id) ?? null;

      if (!obm) return Promise.resolve(new Map());

      /** @type Map<BucketSerialSmall, BloomTiny16bits> */
      const bm = obm;

      return Promise.resolve(bm);
    };
  };

  /** @type IO<Array<BucketSerialSmall>> */
  const ibnums = getBucketNumbers(
    filterInfo,
    filter2hashBucketNew(),
    bloom2resultTiny,
    date,
    shipId,
    bloomSource,
  );

  /** @type Array<BucketSerialSmall> */
  const bnums = await ibnums();

  for (const bno of bnums) {
    console.info(`candidate bucket serial: ${bno}`);
  }

  const b0 = bnums[0];

  const fname = `./sample.d/${shipId}/${date}/${b0}.jsonl`;

  /** @type string */
  const lines = await readFile(fname, Object.freeze({ encoding: "utf8" }));

  /** @type string[] */
  const splited = lines.split(/\n/).filter((line) => !!line);

  /**
   * @typedef {object} OrderItem
   * @property {number} order
   * @property {number} user
   * @property {number} unixtime_ms
   */

  /** @type function(string): OrderItem */
  const str2obj = (line) => {
    const raw = JSON.parse(line);
    return Object.freeze({
      order: raw.order,
      user: raw.user,
      unixtime_ms: raw.unixtime_ms,
    });
  };

  /** @type OrderItem[] */
  const parsed = splited.map(str2obj);

  for (const itm of parsed) {
    console.info(itm);
  }

  return Promise.resolve();
};

main().catch(console.error);
