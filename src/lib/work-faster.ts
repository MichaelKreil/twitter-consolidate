"use strict"

import { createReadStream, statSync } from 'node:fs';
import http from 'node:http';
import https from 'node:https';
import os from 'node:os';
import { extname } from 'node:path';
import { Transform } from 'node:stream';
import { StringDecoder } from 'node:string_decoder';
import { createGunzip, createBrotliDecompress } from 'node:zlib';

interface Wrapper<T> {
	forEachAsync: (callback: (element: T, index: number) => Promise<void>, maxParallel?:number) => Promise<void>
}

export function WF<T>(array: T[]): T[] & Wrapper<T> {
	const newArray = array as  T[] & Wrapper<T>;
	newArray.forEachAsync = forEachAsync;
	return newArray;

	async function forEachAsync(callback: (element: T, index: number) => Promise<void>, maxParallel?:number): Promise<void> {
		const concurrent = maxParallel ?? os.cpus().length;

		return new Promise((resolve, reject) => {
			let running = 0, index = 0, finished = false;

			queueMicrotask(next);

			function next() {
				if (finished) return;
				if (running >= concurrent) return;
				if (index >= array.length) {
					if (running === 0) {
						finished = true;
						resolve();
						return
					}
					return
				}

				running++;
				let currentIndex = index++;

				callback(array[currentIndex], currentIndex)
					.then(() => {
						running--;
						queueMicrotask(next)
					})
					.catch(err => {
						finished = true;
						reject(err);
					})

				if (running < concurrent) queueMicrotask(next);
			}
		})
	}
}
/*
export async function streamFileData(filename, opt) {
	if (!opt) opt = {};

	let stream, size;

	if (filename.startsWith('http://')) {
		stream = await getHttpStream(http, filename)
	} else if (filename.startsWith('https://')) {
		stream = await getHttpStream(https, filename)
	} else {
		size = statSync(filename).size;
		stream = createReadStream(filename);
	}
	if (stream && stream.headers && stream.headers['content-length']) {
		size = parseInt(stream.headers['content-length'], 10);
	}

	if (opt.progress && size) {
		let pos = 0;
		let progress = (typeof opt.progress === 'function') ? opt.progress : createProgressBar;
		progress = progress(size);
		stream.on('data', chunk => {
			pos += chunk.length;
			progress.update(pos);
		})
		stream.on('close', () => progress.close());
	}

	if (filename.endsWith('.gz')) {
		filename = filename.slice(0, -3);
		stream = stream.pipe(createGunzip())
	} else if (filename.endsWith('.br')) {
		filename = filename.slice(0, -3);
		stream = stream.pipe(createBrotliDecompress())
	}

	if (/\.[ct]sv$/.test(filename)) {
		if (opt.fast) {
			return getCsvParserFast(stream);
		} else {
			return getCsvParser(stream);
		}
	} else if (/\.(geo)?json(l|seq)$/.test(filename)) {
		return getJsonParser(stream);
	} else {
		throw new Error('Unknown file extension: ' + extname(filename));
	}

	function getHttpStream(lib, url) {
		return new Promise(res => {
			lib.request(url, stream => res(stream)).end();
		})
	}

	function getSplitter(matcher = /\r?\n/, format = 'utf8') {
		let last = '';
		let decoder = new StringDecoder(format);

		return new Transform({
			autoDestroy: true,
			readableObjectMode: true,
			transform,
			flush,
		})

		function transform(chunk, enc, cb) {
			last += decoder.write(chunk);
			let lines = last.split(matcher);
			let lastIndex = lines.length - 1;
			for (let i = 0; i < lastIndex; i++) this.push(lines[i])
			last = lines[lastIndex];
			cb();
		}

		function flush(cb) {
			this.push(last);
			cb();
		}
	}

	function getCsvParserFast(stream) {
		let header, separator;

		stream = stream.pipe(getSplitter());
		return stream.pipe(new Transform({
			autoDestroy: true,
			readableObjectMode: true,
			transform: (line, enc, cb) => {
				line = line.toString();
				if (header) {
					if (line.length < 1) return cb();
					line = line.split(separator);
					cb(null, Object.fromEntries(header.map((key, i) => [key, line[i]])));
				} else {
					separator = ',';
					header = line.split(separator);

					if (line.split(';').length > header.length) {
						separator = ';';
						header = line.split(separator);
					}

					if (line.split('\t').length > header.length) {
						separator = '\t';
						header = line.split(separator);
					}
					cb(null);
				}
			},
		}))
	}

	function getCsvParser(stream) {
		let parser = papa.parse(papa.NODE_STREAM_INPUT, { header: true });
		return stream.pipe(parser);
	}

	function getJsonParser(stream) {
		stream = stream.pipe(getSplitter());
		return stream.pipe(new Transform({
			autoDestroy: true,
			readableObjectMode: true,
			transform: (line, enc, cb) => {
				cb(null, (line.length > 0) ? JSON.parse(line) : null)
			},
		}))
	}
}

export function createProgressBar(total, timeStep = 1000) {
	const MAX_STATES = 30;
	let index = 0;
	let nextUpdateTime = Date.now();
	const previousStates = [];
	update(0);

	return { update, close, increment };

	function log() {
		let time = Date.now();
		const progress = 100 * index / total;
		let message = `\r\x1b[K   ${index}/${total} - ${progress.toFixed(2)} %`;

		const newState = { index, time };
		const lastState = previousStates[previousStates.length - 1] || newState;
		if ((lastState.index !== index) || (lastState === newState)) {
			previousStates.unshift(newState);
			while (previousStates.length > MAX_STATES) previousStates.pop();
		}

		if (lastState.index < index) {
			const speed = 1000 * (index - lastState.index) / (time - lastState.time);
			let speedString;
			if (speed >= 1e6) {
				speedString = (speed / 1000).toFixed(0) + ' K'
			} else if (speed >= 1e5) {
				speedString = (speed / 1000).toFixed(1) + ' K'
			} else if (speed >= 1e4) {
				speedString = (speed / 1000).toFixed(2) + ' K'
			} else if (speed >= 1e3) {
				speedString = speed.toFixed(0)
			} else if (speed >= 1e2) {
				speedString = speed.toFixed(1)
			} else if (speed >= 1e1) {
				speedString = speed.toFixed(2)
			} else {
				speedString = speed.toFixed(3)
			}

			const eta = (total - index) / speed;
			const etaString = [
				Math.floor(eta / 3600).toFixed(0),
				(Math.floor(eta / 60) % 60 + 100).toFixed(0).slice(1),
				(Math.floor(eta) % 60 + 100).toFixed(0).slice(1),
			].join(':');

			message += ` - ${speedString}/s - ${etaString}`;
		}

		process.stderr.write(message);
	}

	function update(value) {
		index = value;
		if (Date.now() >= nextUpdateTime) {
			log();
			nextUpdateTime += timeStep;
		}
	}

	function increment(value = 1) {
		update(index + value);
	}

	function close() {
		index = total;
		log();
		process.stderr.write(`\n`);
	}
}

*/